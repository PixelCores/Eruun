# 现有集群应用导入分析

> 状态：Historical / Audit。本文基于 `master` 提交 `05c48ad6`（2026-08-03）分析现有 Kubernetes 集群应用进入 Eruun 的发现、导入、接管和运行边界。文中的“当前实现”和“当前限制”均指该历史基线，不代表后续 `master`；现行对外契约以 `import-existing-namespace-api.md`、`application-management-mode.md` 和 `application-status-api.md` 为准。

## 结论

截至该基线，实现已经具备两条用途不同的 Namespace 导入路径：

- `observe` 适合把存量资源转换为 Eruun 元数据并只读观察。它不是“零 Kubernetes 写入”：`apply` 会写应用、组件和禁用的 workflow，并为允许导入的资源补充 Eruun 标签；“只读”描述的是导入后的应用管理权限。
- `adopted` 适合在不替换原资源身份、不由导入请求直接触发 rollout 的前提下接管明确选中的 Deployment 或 StatefulSet。它要求调用方先显式提供应用/组件到 workload 的映射，再通过签名 dry-run/apply 完成接管。

因此，若目标是“分析一个现有 Namespace 中有哪些应用”，`/try` 只能作为旧分组规则的候选发现和孤儿检查，不能当作通用应用识别器，也不能直接授权接管。若目标是“以后由 Eruun 安全管理原工作负载”，应使用显式 `adopted` dry-run，逐项审查 source identity、dependency role、ownership 和 disposition 后再 apply。

这两条路径均不自动执行应用 workflow。导入会创建或更新 workflow 定义；只有后续显式执行 workflow 时才产生 `taskId` 和任务状态。

## 分析范围与事实源

本文检查了该基线下的以下实现链路：

`route -> DTO -> handler -> namespace import domain -> application transaction -> model/repository -> workflow/job runtime -> tests -> Current docs`

主要代码锚点：

- 路由与 Handler：`pkg/apiserver/interfaces/api/applications.go`、`application_lifecycle.go`
- 对外 DTO：`pkg/apiserver/interfaces/api/dto/v1/types.go`、`namespace_import_request_json.go`
- 扫描、旧分组和导入编排：`pkg/apiserver/domain/service/namespaceimport/namespace_import.go`、`namespace_import_scan.go`
- 显式接管规划与持久化：`pkg/apiserver/domain/service/namespaceimport/adopted_import.go`
- 应用、组件和内部 source binding：`pkg/apiserver/domain/model/applications.go`
- source-aware workflow/job：`pkg/apiserver/event/workflow/job_builder_adopted_dependencies.go`、`pkg/apiserver/event/workflow/job/job_adopted_source.go`、`job_adopted_policy.go`
- 生命周期、版本更新和清理：`pkg/apiserver/domain/service/application/application_workload_adopted.go`、`application_adopted_version_contract.go`、`application_adopted_cleanup.go`
- Pod 归属协调：`pkg/apiserver/infrastructure/adoption/`
- Secret 与计划签名：`pkg/apiserver/security/importsecret/`

Namespace import handler 直接返回 Domain DTO，不经过 assembler；该层在本链路中保持不变。本文未连接真实集群、未执行生产 dry-run/apply，也未检查 PaaS 等其他仓库的订单或 `deploy_state` 语义。

## 入口与实际写入边界

| 入口 | 主要用途 | DB 读取 | DB 写入 | Kubernetes 写入 |
| --- | --- | --- | --- | --- |
| `POST /applications/import/namespace/try` | 旧规则候选分组、conversion 预览、已导入应用孤儿识别 | 是 | 否 | 否 |
| `POST /applications/import/namespace`，`observe + dry-run` | 预览旧导入计划 | 是 | 否 | 否 |
| `POST /applications/import/namespace`，`observe + apply` | 创建/替换只读应用元数据 | 是 | 是 | 是，给未阻断、非数据保护资源补标签 |
| `POST /applications/import/namespace`，`adopted + dry-run` | 对显式 root 映射构建依赖闭包和签名计划 | 是 | 否 | 否 |
| `POST /applications/import/namespace`，`adopted + apply` | 验证指纹和漂移，原子保存接管状态 | 是 | 是，单事务 | 导入请求本身不写；binding 提交后 Pod coordinator 可异步补 Pod metadata 标签 |
| adopted 后续 lifecycle/workflow/version | 管理已经接管的 source workload 和允许写入的依赖 | 是 | 是 | 是，执行前再次按 source UID、snapshot 和 disposition 校验 |

所有 Namespace import 入口都拒绝 `default` Namespace。`adopted` 还必须显式提供 `mode`；省略 `managementMode` 的旧请求继续进入 `observe`，不会隐式升级为可写接管。

## 发现与分组能力

### 扫描范围

该基线扫描器可读取 18 类资源：Deployment、StatefulSet、DaemonSet、Job、CronJob、ConfigMap、Secret、PVC、Service、Ingress、ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding、PodDisruptionBudget、NetworkPolicy 和 PV。

- 旧 `observe` 路径默认扫描前 13 类，也可用 `includeKinds` 收窄或显式选择其他已支持类型。
- 显式 `adopted` 规划固定扫描全部 18 类，用于计算 root workload 的依赖闭包；调用方不能用 `includeKinds` 缩小安全检查范围。
- Namespace、CRD 和自定义资源不在该基线的扫描/接管边界中。

### 旧 `/try` 的分组规则

`/try` 与 `observe` 共用旧分组逻辑，其 app 归属优先级为：

1. 资源标签 `eruun.io/import-app-key`；
2. 资源标签 `eruun.io/app-id`；
3. 能严格解析出 16 或 24 位生成式 app ID 的资源名；
4. Service selector、Ingress backend、PodSpec 引用、PVC 前缀、ServiceAccount/RBAC 等依赖关系；
5. 无法安全归属的资源进入 Namespace 级 shared 分组。

这套逻辑对曾由 Eruun 创建或已经带 Eruun 标签的资源最有效。对只有普通业务命名、没有稳定 app ID 或管理标签的任意第三方集群，它无法可靠判断业务应用边界。因此：

- `/try` 返回的 `apps`、`scannedComponents` 和 warning 是候选分析，不是所有权事实；
- `/try` 不产生 adopted `planFingerprint`；
- 不能把同一 Namespace、相似名称或 selector 命中自动解释为“允许 Eruun 接管”。

### 显式 adopted 映射

adopted 不复用旧分组结果作为授权。请求必须明确给出一个应用映射，以及每个组件对应的 `apps/v1` Deployment 或 StatefulSet 名称。该基线下一次请求只能包含一个应用映射，组件名和 workload identity 必须唯一。

该设计把“业务上哪些 workload 属于一个应用”的决定留给调用方，把 Eruun 的自动分析限制在所选 root 的依赖闭包、共享关系和安全处置。这是该基线实现避免误接管的核心边界。

## Adopted dry-run 分析内容

显式 dry-run 会对选中的 root workload 和依赖执行以下分析：

1. 读取 workload 的 `apiVersion/kind/namespace/name/uid/resourceVersion`，并计算规范化 spec digest。
2. 从 PodTemplate、Service selector、Ingress backend、存储、配置、Secret、ServiceAccount/RBAC 和策略对象收集依赖闭包。
3. 为资源标记 dependency role，例如 workload、service、ingress、config、secret、pvc、service-account、rbac、pdb 或 network-policy。
4. 判断 ownership：`exclusive`、`shared`、`external` 或 `data-protected`。
5. 判断 disposition：`managed`、`shared-preserved`、`data-protected`、`blocked` 或 `excluded`。
6. 把 source identity、依赖图、目标应用/组件/workflow 的规范化 DB 状态纳入 canonical plan，并用 keyring 生成 HMAC fingerprint。

以下情况会使计划 fail-closed：

- root 被 HPA 管理；
- source 缺少 UID/resourceVersion，或 source UID 已被其他组件绑定；
- PodTemplate、ReplicaSet 或 Pod 存在冲突或不完整的 Eruun 管理标签；
- ownerReference 与拟接管资源形成不安全所有权关系；
- 依赖跨显式应用共享，或发现无法安全归属的外部使用；
- 目标应用、组件或 workflow 在 dry-run 后漂移；
- import Secret keyring 未配置或 fingerprint 无法验证。

PVC/PV 始终按数据资源保护。ClusterRole、ClusterRoleBinding 及 shared/external 资源只作为依赖上下文保留，不进入可删除所有权。

## Apply、持久化与幂等

adopted apply 会重新扫描当前 Kubernetes 状态并重建 canonical plan。只有 dry-run 返回的 fingerprint 仍能验证，且 source UID、resourceVersion、spec digest、依赖图和目标 DB 状态均未漂移时，才进入写事务。

同一个应用事务中保存：

- `Applications.management_mode=adopted`；
- 内部 `adoption_snapshot`；
- 应用组件与默认/更新 workflow；
- 每个 root 组件的 source `apiVersion/kind/name/UID`；
- source Pod selector、可恢复副本数和 Secret 密文；
- 目标 observe/adopted 应用替换时可复用的组件数字 ID。

`source_workload_uid` 有唯一索引，防止同一 Kubernetes workload 被多个组件接管。Secret 值不进入公开 DTO、日志或 fingerprint；数据库保存的是以 app/resource/key AAD 绑定的 AES-256-GCM envelope。

完全相同的 apply 可作为幂等 replay 返回，不重复写数据库或 Kubernetes。若已持久化 snapshot、source binding、目标 DB 状态或当前集群依赖图不再一致，则返回 plan drift，调用方必须重新 dry-run。

## 导入后的执行边界

### Observe

observe 应用的 workflow 会在导入事务中禁用，并删除调度配置。应用替换、workflow 编辑/执行、版本更新、数据库重置、start/stop/restart、资源清理和 Pod shell 写操作均被拒绝。删除应用只删除 Eruun 元数据并保留 Kubernetes 资源。

### Adopted

adopted 应用保留可执行 workflow，但执行链不会按 Eruun 生成名称猜测资源：

- Deployment/StatefulSet 始终按持久化 source identity 定位；同名不同 UID 是冲突。
- Service、Ingress、ConfigMap、Secret、PVC、ServiceAccount、Role、RoleBinding、PDB 和 NetworkPolicy 从 snapshot 补成 source-bound job。
- shared、external、data-protected、blocked、excluded 和 cluster-scoped RBAC 不进入普通可写集合；受保护的 standalone PVC 只允许显式扩容，不允许缩容或缺失重建。
- Job 入队时检查 management mode，Job 真正执行前再次读取 DB 校验，避免排队期间模式变化后继续写集群。
- database reset 和无签名 cleanup 被拒绝；版本更新只接受已有 adopted 调和器明确支持的字段。
- stop/start/restart 在应用级调度锁中重新检查 HPA、source UID、StatefulSet/PVC 安全状态，并使用实际副本快照恢复。
- 日志、容器、文件与 exec 通过 owner UID 链选择 Pod，不信任普通同名 Pod 或可伪造标签。

默认删除 adopted 应用只解除绑定。显式删除 exclusive 非数据资源必须先生成 cleanup plan，再提交匹配 fingerprint；cleanup 会重新扫描 runtime children，并按 UID precondition 删除。PVC/PV、shared/external 资源始终保留。

## ID、状态与事实源

| 字段 | 本链路的来源与写入点 | 读取方 | 说明 |
| --- | --- | --- | --- |
| `appId` | `Applications.ID`，导入事务创建或复用 | API、组件、workflow、runtime 查询 | Eruun string ID；是应用主事实，不等同 Kubernetes UID |
| 组件 `id` | `ApplicationComponent.ID` | 标签关联、组件 API | DB 数字 ID；替换目标应用时按同名组件复用 |
| source UID | `ApplicationComponent.SourceWorkloadUID` | adopted lifecycle、job、Pod 查询、coordinator | Kubernetes identity；唯一索引；同名不同 UID 不可替代 |
| `workflowId` | 导入事务 upsert 的 `Workflow.ID` | workflow API 和后续执行 | Eruun string ID；导入本身不入队 |
| `taskId` | 后续执行时创建的 `WorkflowQueue.TaskID` | controller、job、状态/取消 API | import dry-run/apply 均不创建 task |
| workflow task `status/state` | `WorkflowQueue.Status` | 调度器、controller、任务 API | DB 是任务事实源；queue 只负责分发 |
| component runtime status | `ApplicationComponent.Status/ReadyReplicas/LastAbnormal` | 应用/组件状态 API | DB 是查询事实源，由 Informer/运行链回写 |
| `deploy_state` | 本仓库无该导入字段或写入点 | 不适用 | 若 PaaS 需要该状态，必须在跨仓库契约中另行确认，不能从 Eruun import 结果推断 |

## 上线前分析与验收建议

### 必须先确认

- 明确目标是只读观察还是后续受控管理；需要 lifecycle/workflow 时选择 adopted。
- 为每个业务应用人工确认 Deployment/StatefulSet root 映射，不直接采用 `/try` 的启发式分组作为所有权结论。
- 配置并备份 import Secret keyring；轮转期间保留仍需验证/解密的旧 key。
- 确认 API Server 和 Pod coordinator 所需 RBAC 已部署，且 leader 生命周期正常。
- 逐项审查 dry-run 的 blocked/warning、shared/external/data-protected 资源和未覆盖的自定义资源。
- 对 HPA 控制的 root、DaemonSet/Job/CronJob 主工作负载以及依赖 CRD/custom resource 的应用先制定显式处置方案。

### 建议验收顺序

1. 在非写入验收实例运行 `/try`，只把结果用于资源盘点和旧应用孤儿识别。
2. 为一个应用编写显式 adopted dry-run 请求，一次只选择已确认的 root。
3. 保存 dry-run 前的 workload UID、generation、PodTemplate hash、ReplicaSet/ControllerRevision 和 Pod UID。
4. 审查所有 `resourceResults`，确认无 error，且每个 disposition 符合预期。
5. 使用原 fingerprint apply，不修改请求映射；发生 409 时重新 dry-run，不绕过漂移检查。
6. 验证 apply 后原 workload UID、generation、PodTemplate hash 和 Pod UID 未因导入改变，DB 中 management mode/source binding/snapshot 已提交。
7. 分别验证状态、日志、容器和文件读取，再在可回滚环境验证 stop/start/restart 与一次 no-op workflow。
8. 最后验证默认删除只 detach，以及 cleanup plan 中 PVC/PV、shared/external 项仍为 retained。

## 基线后的重要演进


## 基线时限制与后续 PR 边界

以下是 `05c48ad6` 基线时的实现限制，不是本文 PR 中要修改的行为：

- 没有面向任意第三方命名规范的通用“自动识别应用”能力；业务应用边界仍需人工确认。
- adopted root 只支持 `apps/v1` Deployment 和 StatefulSet；其他 workload 只能作为 observe conversion 能力的一部分，不能作为 adopted root。
- 一次 adopted 请求只处理一个应用映射，不提供 Namespace 级批量原子接管。
- HPA 控制的 root 在该基线直接阻断，而不是冻结、迁移或共同管理 HPA。
- Namespace、CRD 和 custom resource 保持 external；它们的健康和生命周期不会由 adopted workflow 接管。
- 单元测试和 fake Kubernetes client 覆盖了主要安全分支，但不能替代目标集群的 admission webhook、operator、finalizer、CNI/CSI 和真实 RBAC 验收。

若后续要新增“分析能力”，建议拆成独立契约 PR：先定义只读 inventory 输出和人工确认边界，再决定是否复用 `/try`。不应让分析结果直接触发 adopted apply，也不应把启发式分组升级为所有权事实。

## 本分析 PR 的行为影响

