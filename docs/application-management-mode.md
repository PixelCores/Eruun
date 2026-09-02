# Application Management Mode

> 状态：Current。本文描述应用写权限边界与显式 namespace 接管契约。

## 模式

`ApplicationBase.managementMode` 返回应用当前管理模式：

- `native`：由 Eruun 创建并完整管理，现有写入、工作流和资源清理行为保持不变。
- `observe`：只读观察。普通应用替换、工作流编辑/执行、版本更新、数据库重置、生命周期操作、资源清理和 Pod Shell 执行都会被拒绝。
- `adopted`：通过显式 namespace dry-run/apply 和签名计划接管既有 workload；后续写操作按 source UID、snapshot 与 disposition 受控执行。

未知的非空模式按 `observe` 处理，避免未来值或脏数据意外获得 `native` 写权限。

## 历史数据迁移

服务启动时执行一次事务迁移：

1. 为缺少 `management_mode` 的应用写入 `native`。
2. 将同时满足 `project=imported`、`version=imported` 的历史 namespace 导入应用改为 `observe`。
3. 在 `system_settings` 写入内部迁移标记，后续启动不再重复分类。

迁移标记不会通过 System Setting 列表 API 暴露。滚动升级期间，旧版本写入的空模式仍由运行时兼容逻辑判定；历史导入记录继续按 `observe` fail-closed。

## Namespace 导入

现有 namespace apply 导入会在应用创建/更新事务内写入 `observe`，并同时禁用该应用的托管工作流。响应中的 `workflowDisabled=true` 是 observe apply 完成这项原子写入的确认位，不表示 adopted 应用全部 workflow 的汇总状态，也不再依赖导入完成后的第二次仓储更新。

显式接管复用 `POST /api/v1/applications/import/namespace`。请求必须包含 `managementMode: "adopted"`、明确的 `dry-run`/`apply` 模式和恰好一个应用映射；apply 还必须携带 dry-run 返回的 HMAC 指纹。只传 namespace 仍走 observe 兼容路径，不会自动接管。完整契约见 `import-existing-namespace-api.md`。

当显式接管通过 `targetAppId` 更新已有应用时，目标必须在应用级调度锁和创建事务内仍为 `observe` 或 `adopted`；`native` 目标会被拒绝。`observe -> adopted` 时，本次 upsert 的默认 workflow 与更新 workflow 会在同一事务内重新启用，其他已禁用 workflow 保持原状；已是 `adopted` 的目标再次 apply 时保留现有 workflow 的 `Disabled` 状态。该边界不会放宽普通应用替换接口的写权限。

删除 `observe` 应用只删除 Eruun 元数据并保留 Kubernetes 资源；如果仍有活动任务，删除会拒绝执行，避免任务与元数据脱钩。

## 执行时保护

工作流入队时会检查模式；Job 真正执行前还会重新读取应用模式。后者用于阻止迁移前已排队、迁移后才开始执行的旧任务修改 Kubernetes 资源。

## Adopted 调和与生命周期

- Deployment 与 StatefulSet 必须使用持久化的 `apiVersion/kind/name/UID` 定位原工作负载，不回退到 Eruun 生成名称。
- Service、Ingress、ConfigMap、Secret、PVC、ServiceAccount、Role 和 RoleBinding 从 adoption snapshot 解析依赖身份；`shared-preserved`、`data-protected` 和 `excluded` disposition 不产生 Kubernetes 写入。
- 同 UID 资源只覆盖 Eruun 可表达且允许修改的字段；内容未变化时为 no-op。adopted 工作负载会保留 live PodTemplate 的 `app.kubernetes.io/managed-by`，即使该键不在工作负载自身 selector 中，也不会切断依赖它的 Service 或 NetworkPolicy。相同名称但 UID 不同会作为所有权冲突拒绝。
- exclusive 非数据资源缺失时可从脱敏 snapshot 安全重建。任何 Kubernetes Create 之前会先在 snapshot v2 中以 CAS 持久化一次性 recreation claim，并把同一 token 写入候选对象；创建后的事务再更新 snapshot、workload UID 或 Secret 密文并清除 claim。若提交失败、结果无法确认或进程本身中断，系统会保留对象和 claim，后续 worker 只接续同 token 的对象，恢复 ownership 后仍会按本次 desired state 完成正常调和；同名但无 token、token 不同或仍在删除中的对象继续按所有权冲突 fail-closed。
- adopted Secret 的 JobInfo 不允许携带明文。托管值只从 AES-256-GCM 密文解密，并绑定应用/资源/key AAD；旧 key 解密成功后通过 CAS 轮转到 active key。
- StatefulSet 更新保留 selector、serviceName、volumeClaimTemplates 等身份和存储字段；独立 PVC 只允许扩容，缺失 PVC 不自动重建。
- workflow worker 会在既有执行范围和审批边界内，把 snapshot 中的 exclusive 依赖补成 source-bound Job；共享、数据保护、排除和 cluster-scoped 资源不会进入写集合。
- workflow 入队和 Job 执行都重新检查 management mode；adopted 禁止 database reset 和无签名 cleanup。版本更新只接受调和器已有明确契约的字段，StatefulSet VCT 改动和任意 traits/properties 替换会 fail-closed。
- stop/start/restart 使用应用级调度锁，并在任何写入前重新读取应用、source UID、HPA 和 PVC 状态。stop 保存 live replicas，start 只恢复该快照；restart 拒绝无法保证滚动替换全部 Pod 的策略。
- 日志、容器、文件和 exec 的 Pod 查找按 source owner UID 链执行，不依赖生成名称或 Eruun 管理标签。
- 删除 adopted 应用默认只删除 Eruun 元数据并返回 `resourcesRetained=true`。显式资源清理必须先调用 `POST /api/v1/applications/:appID/resources/cleanup-plan` 生成 HMAC 签名计划，再向 `DELETE /api/v1/applications/:appID/resources` 提交匹配指纹。
- signed cleanup 会先停止 root workload 并重扫 ReplicaSet/ControllerRevision/Pod，再按 UID 前置条件删除 exclusive 非数据资源。PVC/PV、shared/external 资源永久保留；漂移、晚到子资源、finalizer 和部分失败保持可重试。
- leader-only Pod coordinator 只加载 adopted source binding，通过 Deployment→ReplicaSet→Pod 或 StatefulSet→Pod 的 owner UID 链确认归属，并只补充 Pod metadata 管理标签。冲突标签不会被抢占，controller PodTemplate 不会被修改；没有 adopted binding 时不创建 namespace watch。

显式 apply 会创建或更新 adopted 应用的默认/更新 workflow，并在同一事务中持久化应用、组件、workflow、snapshot、source binding 与 Secret 密文；导入本身不修改 Kubernetes。上述能力不会改变 namespace-only 导入的 `observe` 兼容行为。
