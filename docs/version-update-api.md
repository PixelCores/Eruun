# 版本更新 API 设计文档

> 状态：Current。当前路由为 `POST /api/v1/applications/:appID/version` 与 `/version/cancel`。

## 概述

版本更新 API 提供了一种简洁、优雅的方式来更新应用版本，支持组件的更新、新增、删除和组件级重启操作，并可通过工作流自动部署。全量清理、全量部署、组件级重启等资源动作也通过本接口的保留组件动作表达，不再放入公开 workflow `jobType`。

## API 端点

```
POST /api/v1/applications/:appID/version
POST /api/v1/applications/:appID/version/cancel
```

## 功能特性

### 更新策略

| 策略 | 值 | 说明 |
|------|------|------|
| 滚动更新 | `rolling` | 默认策略，逐步替换 Pod，保证服务可用性 |
| 重建更新 | `recreate` | 先删除所有旧 Pod，再创建新 Pod |
| 金丝雀更新 | `canary` | 先更新部分 Pod，验证后再全量更新 |
| 蓝绿部署 | `blue-green` | 创建新版本，切换流量后销毁旧版本 |

### 组件操作

| 操作 | 值 | 说明 |
|------|------|------|
| 更新 | `update` | 默认操作，更新现有组件的配置 |
| 新增 | `add` | 向应用添加新组件 |
| 删除 | `remove` | 从应用移除组件 |
| 重启 | `restart` | 不修改组件规格，通过 workflow task 对现有组件触发 rollout restart |

`remove` 规则：带 `traits.share` 且策略为 `default` / `ignore` 的共享组件不能通过版本更新移除；`share=force` 按普通组件处理，仍允许移除。

组件 action 边界行为：
- `action` 为空时按 `update` 处理；非空值只能是 `update`、`add`、`remove`、`restart`。
- `update` 的目标组件必须已存在；不存在时返回 `10013`。
- `add` 的目标组件必须不存在；已存在时返回 `10015`。
- `remove` 的目标组件必须已存在；不存在时返回 `10013`。
- `restart` 的目标组件必须已存在，且仅支持 `webservice` / `store`，分别触发 Deployment / StatefulSet rollout restart；patch 成功后 workflow 还会等待本次 restart 产生的新 Pod Ready。
- `{"action":"remove","name":"cleanup_all"}` 是保留资源动作，表示通过 workflow task 清理当前应用全部 DB 已知组件的普通 Kubernetes 运行资源，不删除组件 DB 记录；standalone PVC 和五类 RBAC 保留。
- `{"action":"add","name":"all"}` 是保留资源动作，表示通过选定 workflow 部署当前应用全部 DB 已知组件，不新增名为 `all` 的组件。
- `cleanup_all` 和 `all` 是 `/version` 中的保留组件名，不能作为普通 `add` / `remove` / `update` 目标。
- 保留资源动作不得携带 `image`、`replicas`、`env`、`type`、`properties`、`traits` 等普通组件字段，否则返回应用配置错误。
- `restart` 也是 task-scoped 资源动作，不修改组件 spec，不允许携带 `image`、`replicas`、`env`、`type`、`properties`、`traits`。
- 同一次请求中，同一组件名不能同时出现 `remove` 和 `update`（空 `action` 也按 `update` 计算）。
- 同一次请求中，同一组件名不能同时出现 `remove` 和 `add`，也不能重复 `remove`。
- 同一次请求中，同一组件名不能重复 `update`；组件名按去除首尾空白并忽略大小写后的逻辑名称判重。
- 同一次请求中，同一组件名不能同时出现 `restart` 和 `update` / `add` / `remove`，也不能重复 `restart`。
- `restart` 不得与 `remove cleanup_all` 或 `add all` 混用，避免清理、重建、重启顺序歧义。
- action 拼写错误（例如 `remvoe`）会返回 `10016`，不会执行版本更新。

## 请求参数

### UpdateVersionRequest

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `version` | string | **是** | 新版本号 |
| `strategy` | string | 否 | 更新策略，默认 `rolling` |
| `executionScope` | string | 否 | workflow 执行范围：`full_workflow`（默认）或 `changed_components` |
| `components` | array | 否 | 组件更新规格列表 |
| `workflowId` | string | 否 | `autoExec=true` 时指定自动执行或 no-op 回调关联的工作流 ID |
| `executeAt` | int64 | 否 | 延迟执行时间（Unix 秒）；仅在 `autoExec=true` 且有组件变更或资源动作并创建 workflow task 时生效 |
| `imageReadyTimeoutSeconds` | int64 | 否 | workload 变更后的 Pod Ready 观测窗口，单位秒；字段名保持兼容；省略或传 `0` 默认 300 秒；负数或大于部署超时时间返回应用配置错误 |
| `autoExec` | bool | 否 | 是否自动执行工作流，默认 `true` |
| `callback` | object | 否 | 本次版本更新任务的终态回调覆盖；`autoExec=true` 且创建 workflow task 时挂到 workflow task，无实际组件/资源变更时挂到本次 update operation task |
| `description` | string | 否 | 更新说明 |

`executeAt` 规则：
- `executeAt < 0`：请求非法，返回工作流配置错误
- `executeAt = 0` 或不传：立即执行
- `executeAt <= 当前时间`：按立即执行处理
- `executeAt > 当前时间`：创建延迟任务，到点执行

`imageReadyTimeoutSeconds` 规则：
- 字段名保持兼容；当前语义是 workload 变更后的 Pod Ready 观测窗口，而不再只表示镜像变更。
- 只有 `autoExec=true` 且创建 workflow task 时才会进入 Pod Ready 观测；`autoExec=false` 仍只写 DB，不创建 workflow、不观测 Pod、不触发 callback。
- `update` 组件时，`webservice` / `store` 的 `image`、`replicas`、`properties`、`env` 或 `traits` 只要实际发生变化，都会记录为 Ready 目标；重复提交同值的 no-op update 不会进入目标集。
- `update` 的 Ready 目标必须被所选 workflow 的 `deploy` 组件步骤覆盖；缺失时请求会在写 DB 或创建 task 前失败，返回 workflow config 错误。
- `add` 组件时，只有新增 `webservice` / `store` 且携带 `image`，才会记录为 Ready 目标。
- `add type=job` 创建的是一次性 Kubernetes Job，不进入 workload Ready 目标集；其完成条件是 batch Job 终态或归属 Pod `Succeeded`，超时使用 workflow Job task 的超时时间。成功完成后会先收集 Job Pod 日志，再以 UID 前置条件删除 Kubernetes Job；确认该 UID 已消失后，才清理仍残留且归属该 UID 的 `Succeeded` Pod。
- `add all` 全量部署资源动作不记录 Ready 目标，也不受 `imageReadyTimeoutSeconds` 约束；其 deploy job 仍使用标准部署超时时间。
- 仅版本号、`config` / `secret` 变更，以及 workload 的 no-op update，不会进入 Ready 目标集。
- 被记录的 Ready 目标会覆盖对应 `deploy` / `store_deploy` job 的超时时间；`restart` 会覆盖对应 `version_restart` job 的超时时间；其他组件、配置、Secret、清理 job 不受影响。
- 成功条件是目标 Pod Ready，而不是仅 `PodPhase=Running`；镜像更新场景还要求目标新镜像对应的 Pod Ready，旧镜像 Ready Pod 不会让镜像更新 job 提前成功。
- 同镜像但 PodTemplate 变化的 Deployment / StatefulSet 更新会在本次目标 PodTemplate 写入 task 标记，并要求带有该标记的目标 Pod Ready；旧 ReplicaSet / ControllerRevision 的 Ready Pod 不会让本次 update 提前成功。
- 如果目标 Pod 在观测窗口内持续 CrashLoopBackOff、ImagePullBackOff 或未 Ready，workflow 会失败或超时，并沿用现有终态 callback 发送 failure / timeout。
- `imageReadyTimeoutSeconds` 表示 Pod 恢复观测窗口；`callback.timeoutSeconds` 只表示单次 callback HTTP 请求超时，两者互不复用。

`executionScope` 规则：
- `full_workflow` 是默认值；不传时保持旧行为，`autoExec=true` 且创建 workflow task 后执行选定 workflow 的完整步骤。
- `changed_components` 只影响本次 `/version` 自动执行 task，不修改 DB 中的 workflow 定义；Eruun 会在 task-scoped `resource_action_info` 中记录本次实际 `updatedComponents + addedComponents`，worker 生成 job 时只保留这些组件命中的 deploy/默认 component jobs。
- `changed_components` 不会自动推断依赖组件。如果本次 backend 更新还需要 config/secret 同步，必须把对应组件也作为本次实际变更提交，或使用默认 `full_workflow`。
- `changed_components` 不能和 `add all` 或 `remove cleanup_all` 组合；这两个资源动作语义上要求全量 workflow。
- `changed_components` 下，若任一本次实际变更组件未被选定 workflow 的可执行组件节点覆盖，请求会在事务内失败并回滚，不会创建“部分组件只改 DB、没有 job reconcile”的 workflow task。
- `changed_components` 不会因为组件同名纳入 `database_reset`、`log_archive_upload` 等非部署 workflow job；需要执行这些动作时应使用对应专用 API/workflow，或使用默认 `full_workflow`。
- 普通 `restart` 仍沿用现有 restart-only 逻辑；restart-only 请求只保留前置审批步骤并追加 `version_restart` job。
- 非法 `executionScope` 会返回应用配置错误，不会静默回退。

`adopted` 兼容性规则：

- workflow 执行仍遵守 `full_workflow` / `changed_components` 的既有裁剪和审批边界；worker 只把 snapshot 中与被保留 workload 关联的 exclusive 依赖补成 source-bound Job。
- workload 更新允许 image、replicas 和合并式字面量 env；任意 properties 替换、未知 traits 变化、StatefulSet VCT 身份/容量变化会在 DB 写入前拒绝。已有 standalone PVC 仅允许在线扩容，最终仍由 live StorageClass/PVC 预检决定。
- `database_reset`、`remove cleanup_all`、普通 remove/add 和无签名资源清理不会对 adopted 应用入队；删除资源必须使用专用 cleanup plan/fingerprint 契约。
- `restart` 会再次校验 source UID；暂停的 Deployment、`OnDelete` StatefulSet、非零 `rollingUpdate.partition` 以及不安全 PVC 状态都会在 PodTemplate 写入前失败。

`autoExec=true` 的执行语义：
- 默认执行选定 workflow 的完整步骤；只有显式传入 `executionScope=changed_components` 时，才按本次实际变更组件裁剪 workflow deploy/默认 component jobs。
- 创建真实 workflow task 时，版本更新与手工执行、到期 schedule、数据库重置共用 per-App 分布式锁；锁内事务原子完成 active task 检查、版本/组件写入和 task 创建。锁竞争返回 `409 / 10031`，且不会提交本次版本、组件或 task 变更；锁只覆盖任务创建事务，不覆盖异步 workflow 的实际执行。
- 当即时请求创建真实 workflow task 后，`GET /api/v1/applications/:appID/status` 与 `POST /api/v1/applications/components/status` 会在该 task 处于 `waiting`、`queued`、`running`、`pending`、`prepare`、`wait_for_approval` 等活跃状态时返回 `updating`，避免旧 Ready 副本让应用聚合状态误报为 `running`。
- `remove cleanup_all` 要求 `autoExec=true` 且 workflow 可执行；请求提交后由 workflow worker 根据 task-scoped `CleanupInfo` 和预创建的 `cleanup_resources` 记录清理普通运行资源，`/version` 接口本身不直接删除 Kubernetes 资源。普通清理保留 standalone PVC 和五类 RBAC，StatefulSet VCT PVC 仍按其现有 retention policy 处理；只有与 `add all` 组合且显式修改非共享 `store` 的不可变 StatefulSet 字段时，才会进入下述受控重建流程。
- `add all` 要求 `autoExec=true` 且 workflow 可执行；选定 workflow 必须覆盖当前全部 DB 已知组件，否则请求失败且不会提交版本或任务。
- `restart` 要求 `autoExec=true` 且 workflow 可执行；restart-only 请求仍会更新应用版本/描述并创建 workflow task，但只保留前置审批步骤，审批后执行重启 job，不执行完整部署 workflow。
- `restart` job 写入 `kubectl.kubernetes.io/restartedAt` 后，会在 `imageReadyTimeoutSeconds` 窗口内等待带有本次注解值的目标 Pod Ready；旧 Ready Pod 不会让重启提前成功。如果新 Pod 在窗口结束时仍持续 CrashLoopBackOff、ImagePullBackOff 或未 Ready，workflow 会失败或超时，并沿用现有终态 callback 发送 failure / timeout。
- `restart` 与普通组件更新混合时，普通 workflow steps 先执行，随后追加组件级重启 job。
- 未变化的核心 Kubernetes 资源保持幂等：`Deployment`、`StatefulSet`、`Service`、`PVC` 在受 Eruun 控制字段归一化后与集群当前状态等价时只记录为已观察资源，不向 Kubernetes 发送更新请求；`Deployment` 与 `StatefulSet` 会忽略 Kubernetes 维护字段和稳定默认字段差异，例如 `status`、`resourceVersion`、`managedFields` 及 Pod 默认 `restartPolicy`、`dnsPolicy`、`schedulerName` 等。
- `/version` 更新 `store` 组件时，会在任何 DB 写入或 workflow task 创建之前渲染更新前后的 StatefulSet 并校验不可变契约。`serviceName`、`selector`、`volumeClaimTemplates` 数量或 VCT 的 `name`、`size`、`storageClass`/其他 spec 发生变化时，请求返回 `HTTP 400 / code 10000`，`message` 会指出组件、不可变字段以及需要显式 StatefulSet/PVC 迁移或重建；响应不会包含更新前后的内部规格值，也不会把新 traits 写入 DB 后再由运行时静默保留旧字段。渲染后的 StatefulSet `namespace/name` 也必须保持不变；版本更新不承载资源身份迁移，例如改变 `traits.share` 导致资源名变化时，即使使用全量重建也会拒绝，调用方必须先独立完成 StatefulSet/PVC 迁移。非 `store` 组件切换 persistent storage 模式时，安全提示只说明 PVC 数据迁移要求，不会误报为 StatefulSet 不可变字段变更。
- `remove cleanup_all + add all` 是上述限制的显式重建路径：只有请求确实修改非共享 `store` 的 `serviceName`、`selector` 或 `volumeClaimTemplates` 时才生成必删 StatefulSet 契约；没有不可变字段变化的普通全量重建继续使用 cleanup v1。只改 `serviceName` / `selector` 且 VCT 不变时使用 cleanup v2；VCT 被新增、移除、改名或同名 VCT spec 变化时使用 cleanup v3，并持久化受影响的旧/新模板名。两种契约都不允许改变 StatefulSet 的 `namespace/name`。
- cleanup v2/v3 worker 会先检查所有带同组件标签的实际 StatefulSet、相关 Pod 及其 owner Job 的实时 share 标签；label 命中但不是契约目标的额外 StatefulSet 也会在资源变更前触发冲突。v3 还会检查计划删除的 PVC，并把首次扫描到的 PVC name/UID 身份清单合并写入当前 cleanup JobInfo；同一 task 重试不会收养同名 replacement 或后来出现的匹配 PVC。通过预检后，worker 把 StatefulSet 是否存在及首次观察到的 UID 写入 Pod 身份 checkpoint，并用该 checkpoint 作为 retention 首轮基线；预检后、第一次 retention GET 前发生的同名 replacement 或从不存在变为出现也会返回冲突。进入 retention mutation 以及最终精确删除 StatefulSet 前，worker 还会刷新固定 UID、实时 share 标签与完整 label-matched target set；预检后新增 `default` / `ignore` 保护或额外 StatefulSet 会在后续 Kubernetes mutation 前 fail-closed。刷新遇到 429、timeout 或 5xx 时会做有限重试，并在每次重试前重新确认持久化 WorkflowQueue 仍为 running；身份、share 或目标集合冲突不会重试。随后 worker 把目标 StatefulSet 的 `persistentVolumeClaimRetentionPolicy.whenDeleted` 和 `whenScaled` 都改为 `Retain`，并等待 StatefulSet controller 观察到新 generation；任一后续收敛轮次发现同名 StatefulSet 已被替换都不会把新对象当作原删除目标。worker 再主动从精确匹配的 VCT PVC 上移除可能触发级联删除的 owner reference：当前 StatefulSet 按 name/UID 匹配，Pod 按 `<statefulset>-<数字 ordinal>` 匹配；worker 会重新读取并确认这些引用全部消失，因此没有现存 Pod 的缩容高 ordinal PVC 也在保护范围内。无法确认收敛时任务超时并 fail-closed，不会继续删除 StatefulSet 或 PVC。
- v2 应保留的全部 VCT PVC，以及 v3 未列入删除计划的 VCT PVC，如果已经带有 `deletionTimestamp`，都会阻止 StatefulSet/Pod 删除；worker 会在 retention 收敛时和删除 Pod 前再次检查，避免 Pod 消失后 `pvc-protection` 解除而静默丢卷。只有 v3 明确列入计划的模板允许已有 PVC 处于删除中。
- 必删 StatefulSet 使用 `Orphan` propagation，避免 Kubernetes 在 worker 完成第二次安全检查前级联删除 Pod。Orphan 前，worker 会按固定 StatefulSet UID 查找 Pod，将 Pod name/UID 以及已验证的 owner Job name/UID 身份清单以 checkpoint 持久化到当前 cleanup JobInfo；因此即使 StatefulSet 删除后 Pod 的 owner reference 或组件标签漂移，或者同一 task 重启时 Pod 已经消失，worker 仍会先按持久化身份收敛 owner Job，再只处理已证明身份且 UID 未变化的 Pod。StatefulSet 消失后，worker 会再次读取每个目标 Pod 及其 owner Job 的实时 share 标签，只显式删除未受保护的对象：owner Job 按 Pod owner reference 固定 UID，并以该次 live GET 的 UID/resourceVersion 和 `Orphan` propagation 删除，等待 Job 真正 `NotFound` 后才进入 Pod 清理，避免活跃 Job 在旧 Pod 终止时创建 replacement。即使 Pod 已带 `deletionTimestamp` 也不会跳过对应 owner Job；owner Job 收敛后只等待该 Pod 消失，不重复发送 Pod Delete。处理 owner Job 后再重读 Pod，以 checkpoint UID 与最新 resourceVersion 删除。任一 Delete conflict 都会重新进入 live identity/share 校验；若对象 UID 未变且仍未受保护，worker 会使用最新 resourceVersion 在既有超时边界内继续收敛，避免控制器正常状态更新把安全清理误判为失败；同名 replacement、未完成 owner Job 捕获的旧 checkpoint、缺少其他可验证身份或任何新出现的 `default` / `ignore` 保护仍会 fail-closed，停止后续 PVC 删除和部署。每次 retention 更新、owner reference patch、StatefulSet/Pod/PVC 删除和 checkpoint CAS 前还会重读持久化 WorkflowQueue；checkpoint 持久化和 Pod 清理阶段同时要求 JobInfo 仍为 running。即使取消状态已提交但 Redis 取消信号发布失败，终态 task 也会关闭破坏路径，并由当前 worker 把仍为 running 的 JobInfo 收敛到相应终态。v2 保留全部 VCT PVC；v3 在 StatefulSet 和相关 Pod 消失后，仅精确扫描并删除计划中的 `<template>-<statefulset>-<数字 ordinal>` PVC（包括缩容后保留的高 ordinal）；每次删除前重读实时 share 标签，并同时携带 checkpoint UID 与该次读取到的 resourceVersion 作为 Kubernetes Delete preconditions，冲突时不会删除 replacement，等到 PVC 真正 `NotFound` 后才部署。相似前缀、其他 StatefulSet、未列入计划的 VCT PVC 和 standalone PVC 不受影响。v3 会删除受影响 VCT PVC 中的数据，调用方应提前备份或迁移。
- 带 `share.strategy=default` / `ignore`（未知策略按 default 处理）的共享组件仍受保护并拒绝必删重建；`share.strategy=force` 不受该保护。cleanup v2/v3 对仅支持 v1 的旧 worker 都是 fail-closed 的未知版本；滚动发布必须先升级全部 worker，再开放可能生成 v2/v3 的不可变 StatefulSet 更新请求。普通无不可变字段变化的全量重建仍使用 cleanup v1，不受该 worker-first 前置影响。
- 已存在的 `Deployment` 需要更新时，Eruun 会保留集群中的不可变 selector，并在默认值归一化后对目标 PodTemplate 做完整差异判断；确认有差异后按目标 PodTemplate 替换受 Eruun 管控的容器、`volumeMounts` 和 `volumes`。因此组件 `traits.storage` 从旧挂载路径或旧 volume source 切换到新目标时，旧主容器挂载和旧 volume source 会被删除；sidecar 自己声明的挂载不受主容器路径替换影响。
- `ConfigMap`、`Secret`、普通 `Job` 的严格 no-op 不属于当前核心资源幂等契约。

`callback` 规则：
- `callback` 复用 App / Workflow Callback 结构，支持 `success`、`failure`、`timeout`、`reject`、`cancelled`、`methods`、`headers`、`timeoutSeconds`。
- 当本次版本更新创建 workflow task 时，`callback` 会作为 task 级快照持久化；终态回调优先级为 `task.callback` > `workflow.callback` > `app.callback`，真实更新的 `success` 会等待对应 workflow task 完成，包括 workload update 或 restart 的 Pod Ready 观测。
- 当 `autoExec=true`、`callback` 非空且无实际组件变更/资源动作时，Eruun 不会为了回调单独重跑 workflow steps，而是把 `callback` 挂到本次已完成的 update operation task，并发送一次 `success` 回调；响应 `taskId` 不是 workflow task。若请求提供了有效 `workflowId`，响应和 callback payload 会回传该 `workflowId`；未提供时保持为空。
- 显式 task callback 需要成功持久化对应 workflow task 或 operation task；no-op 更新的 App 版本与 operation task 在同一事务提交，如果 task 创建失败则版本保持旧值、请求返回错误且不会尝试发送 callback。
- task 级 `callback` 不会写入 App，也不会覆盖 Workflow 默认 callback；后续 workflow 执行不受本次请求影响。
- `autoExec=false` 时，`callback` 被忽略，不校验也不触发。
- `autoExec=true` 且 `callback` 生效时，Callback URL 受 `urlSecurityPolicy` 校验；非法 URL 会导致版本更新失败且不提交应用版本、组件或 workflow task。

### CancelDelayedVersionUpdateRequest

用于取消尚未执行的延迟版本更新任务（必须提供 `taskId`）：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `taskId` | string | **是** | 需要取消的任务 ID |
| `user` | string | 否 | 取消人；不传使用系统默认值 |
| `reason` | string | 否 | 取消原因 |

删除清理行为：`autoExec=true` 的版本更新会在删除组件 DB 记录前，先把被删除组件解析为脱敏的资源清理计划并写入工作流 Task 记录；随后预创建最小资源清理记录用于状态跟踪。`autoExec=false` 不创建工作流或 Task 清理计划，但普通 `remove` 仍会在接口调用内同步清理该组件的普通 Kubernetes 运行资源，再删除组件 DB 记录。`remove cleanup_all` 不删除组件 DB 记录，也不会由 `/version` 接口直接删除 Kubernetes 资源；它要求创建 workflow task，并为当前应用全部 DB 已知组件生成 task-scoped `CleanupInfo` 与预创建的 `cleanup_resources` 记录，后续由 workflow worker 执行。同步 cleanup 和普通 workflow cleanup 都会保留 standalone PVC、显式 `claimName` PVC、标签命中的 PVC、命名空间共享日志 PVC，以及 ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding；普通 StatefulSet 删除时 VCT PVC 生命周期仍由原 retention policy 决定。显式不可变 StatefulSet 重建是唯一例外：cleanup v2/v3 会先强制收敛到 `Retain/Retain` 再删除 StatefulSet，v2 保留全部 VCT PVC，v3 仅按持久化模板名删除受影响的 VCT PVC 并等待其消失。清理计划仅记录资源命名、受影响的 VCT 模板名和清理所需字段（包含 `traits.storage`、`traits.init[].traits.storage`、`traits.sidecar[].traits.storage`），不会持久化环境变量、配置、Secret、Cloud 参数或命令等敏感 payload。

终态行为：当延迟版本更新或审批挂起的版本更新被取消，或工作流在执行到删除组件清理步骤前进入失败、超时等终态时，系统会将该任务下由版本更新删除组件预创建、且尚未开始执行的资源清理记录同步置为对应终态，避免已结束任务继续保留活跃清理记录。如果父 WorkflowQueue 终态已经落库，但该同步步骤因进程退出或临时存储错误未完成，历史扫描会把仍为空、`created`、`queued`、`waiting` 或 `pending` 的预创建 JobInfo 视为尚未执行的 pending 契约，而不会永久误报为活跃清理；`prepare`、`running` 等已开始状态仍会阻止恢复。

对于 `remove cleanup_all + add all` 触发的 cleanup v2/v3 StatefulSet 迁移，系统会从历史 task 持久化识别 pending 契约。只有对应 `cleanup_resources` JobInfo 与其整体 WorkflowQueue task 都为 `completed`，该组件的契约才会精确解除；cleanup Job 已完成但后续 deploy 失败、Workflow 或 Job 为 `passed` / `skipped`，以及任一方进入 `failed`、`timeout`、`cancelled` 或 `reject`，都继续保留 pending，因为这些状态不能证明整次迁移和重部署已经成功。恢复 task 会在 `cleanup_info.resolvesTaskIDs` 中显式引用它要消解的未完成 task；系统先独立验证全部 v2/v3 契约，再只允许整体成功且组件、资源身份和 VCT 计划完整覆盖引用目标的恢复 task 解除 pending。引用缺失、重复、自引用、循环或覆盖不完整都会 fail-closed；判断不依赖 API 实例的本地 `create_time` 或查询返回顺序，因此跨实例时钟偏差和同毫秒时间戳不会颠倒恢复因果。历史上没有显式引用的成功 task 不会猜测性消解旧失败 task，需要再提交一次合法恢复。恢复请求必须继续携带 `remove cleanup_all + add all`，为每个 pending 组件重放唯一且确实产生相应不可变字段变化的 `update`：v2 重放 StatefulSet 不可变字段变更；v3 的 historical descriptor 到本次 desired StatefulSet 的 transition 必须覆盖每个 pending VCT 模板，不能通过只修改其他无关 VCT 来绕过原迁移。v3 还会恢复尚未完成的模板名和旧资源描述；缺少任一组件、部分重放、只改镜像或改变 StatefulSet `namespace/name` 都会 fail-closed。

在 pending 解除前，其他携带组件或资源动作的版本更新、直接 workflow 执行、定时调度部署、数据库重置、`DELETE /applications/:appID/resources` 公共资源清理，以及通过 `POST /applications` 或 `POST /applications/create-and-exec` 对既有 ID / 已命中 template key 的应用执行整体刷新，都会被拒绝或跳过；纯版本号或描述更新仍可执行。既有应用刷新和公共资源清理会先解析数据库中的 canonical app ID，再与版本更新、直接 workflow 入队、定时调度和数据库重置共用 app-scoped 分布式锁。刷新路径在任何 namespace/Kubernetes 副作用前做一次 idle/pending fail-fast，并在刷新事务内重新读取应用、再次检查栅栏，之后才可替换应用组件或 workflow。公共资源清理也会在锁内重读应用并先检查 idle/pending，任何组件状态、Kubernetes 资源或 operation task 写入都在检查之后。因而 pending 刷新不会改变应用、组件 numeric ID 或 workflow，`create-and-exec` 也不会继续入队；pending 公共清理不会触发 Kubernetes 删除。显式级联删除整个应用是终止性操作：它在同一锁内先取消活跃任务，然后有意跳过 pending 恢复栅栏并完成资源与元数据删除。全新应用创建不获取既有应用锁，不依赖 Redis locker。已提前创建的普通延迟 workflow 还会在到点执行、从 `waiting` 进入 `queued` 前，在同一应用锁内重新检查 pending 栅栏；cleanup v2/v3 迁移或恢复任务本身可通过该检查。锁键对 app ID 做 trim 和大小写归一化；锁冲突返回 409/10031，锁后端不可用返回 503/10032，pending v2/v3 栅栏返回 400/10000 并携带可安全展示的恢复提示，活跃 cleanup 返回 409/20007，取消收敛中返回 409/20008。task 与 JobInfo 合同不一致时也会 fail-closed。

并发行为：延迟任务取消与调度器的 `waiting -> queued` 迁移通过原子状态条件竞争。只有仍处于 `waiting` 的任务会被取消；如果调度器已经取得执行权，取消接口返回 HTTP 409 / 业务码 10033，且不会把已开始执行的任务覆盖为 `cancelled`。

### ComponentUpdateSpec

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `action` | string | 否 | 操作类型：`update`/`add`/`remove`/`restart`，默认 `update` |
| `name` | string | 是 | 组件名称 |
| `image` | string | 否 | 新镜像地址 |
| `replicas` | int32 | 否 | 新副本数；显式提供时必须大于 `0`，`/version` 不支持 scale-to-zero。`webservice` 停服使用 lifecycle stop API；`store` 停服需使用显式维护流程 |
| `env` | object | 否 | 环境变量覆盖（合并更新） |
| `type` | string | 否 | 组件类型（新增时必填）：`webservice`/`store`/`config`/`secret`/`job` |
| `properties` | object | 否 | 组件属性（新增时可选） |
| `traits` | object | 否 | 组件特性（新增时可选） |

## 响应参数

### UpdateVersionResponse

| 字段 | 类型 | 说明 |
|------|------|------|
| `appId` | string | 应用 ID |
| `version` | string | 新版本号 |
| `previousVersion` | string | 更新前版本号 |
| `strategy` | string | 使用的更新策略 |
| `executionScope` | string | 使用的 workflow 执行范围 |
| `taskId` | string | 工作流任务 ID；未触发工作流时返回版本更新任务 ID |
| `workflowId` | string | 本次关联的工作流 ID；真实 workflow task 返回实际执行 workflow，no-op operation task 仅在请求提供有效 `workflowId` 时返回 |
| `updatedComponents` | array | 已更新的组件名称列表 |
| `addedComponents` | array | 新增的组件名称列表 |
| `removedComponents` | array | 已删除的组件名称列表 |
| `restartedComponents` | array | 已接受并入队重启的组件名称列表 |

## 错误码

| HTTP 状态码 | 业务码 | 说明 |
|------------|-------|------|
| 400 | 10000 | 应用配置错误；StatefulSet 不可变字段变更或历史 cleanup v2/v3 仍 pending 时，`message` 包含对应迁移/恢复提示 |
| 404 | 10005 | 应用不存在 |
| 500 | 10011 | 版本更新失败 |
| 400 | 10012 | 无效的更新策略 |
| 404 | 10013 | 组件不存在 |
| 400 | 10014 | 没有可更新的组件 |
| 400 | 10015 | 组件已存在（新增时） |
| 400 | 10016 | 无效的组件操作类型 |
| 409 | 10031 | 同一应用存在并发的版本更新、工作流入队、调度、数据库重置或其他互斥操作；调用方应稍后重试 |
| 503 | 10032 | 应用级分布式锁后端不可用；请求不会继续写入版本或任务 |
| 409 | 10033 | 版本更新任务不可取消（已执行/非延迟待执行） |
| 404 | 20005 | 指定的工作流不存在 |
| 409 | 20007 | 应用已有运行中或排队中的工作流/StatefulSet 清理任务 |
| 409 | 20008 | 应用工作流正在取消且清理 Job 尚未收敛 |

---

## JSON 请求示例

### 1. 简单镜像更新

更新单个组件的镜像版本：

```json
{
  "version": "1.1.0",
  "strategy": "rolling",
  "imageReadyTimeoutSeconds": 300,
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ]
}
```

**响应示例：**

```json
{
  "appId": "abc123xyz",
  "version": "1.1.0",
  "previousVersion": "1.0.0",
  "strategy": "rolling",
  "executionScope": "full_workflow",
  "taskId": "task-def456",
  "updatedComponents": ["backend"],
  "addedComponents": [],
  "removedComponents": [],
  "restartedComponents": []
}
```

### 2. 扩容副本数

仅扩容某个组件的副本数：

```json
{
  "version": "1.0.1",
  "components": [
    {
      "name": "backend",
      "replicas": 5
    }
  ]
}
```

### 3. 更新多个组件

同时更新多个组件的镜像和配置：

```json
{
  "version": "2.0.0",
  "strategy": "rolling",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v2.0.0",
      "replicas": 3
    },
    {
      "name": "frontend",
      "image": "myapp/frontend:v2.0.0"
    }
  ],
  "description": "Major version upgrade"
}
```

### 4. 新增组件

向应用添加新的组件（如 Redis 缓存）：

```json
{
  "version": "2.0.0",
  "components": [
    {
      "action": "add",
      "name": "redis-cache",
      "type": "store",
      "image": "redis:7-alpine",
      "replicas": 1,
      "properties": {
        "ports": [{"port": 6379}]
      }
    }
  ]
}
```

### 5. 删除组件

从应用中移除不需要的组件：

```json
{
  "version": "2.0.0",
  "components": [
    {
      "action": "remove",
      "name": "legacy-service"
    }
  ]
}
```

### 6. 混合操作（更新 + 新增 + 删除）

在一次请求中同时执行多种操作：

```json
{
  "version": "3.0.0",
  "strategy": "rolling",
  "components": [
    {
      "action": "update",
      "name": "backend",
      "image": "myapp/backend:v3.0.0"
    },
    {
      "action": "add",
      "name": "message-queue",
      "type": "store",
      "image": "rabbitmq:3-management",
      "replicas": 1
    },
    {
      "action": "remove",
      "name": "deprecated-worker"
    }
  ],
  "autoExec": true,
  "description": "Architecture refactoring"
}
```

### 7. 指定自动执行的工作流

当 `autoExec=true` 且有组件变更或资源动作并会创建 workflow task 时，可指定 `workflowId` 执行目标工作流：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-custom-update",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ]
}
```

### 8. 复用 workflow 但只执行本次变更组件

当 PaaS 只更新单个 webservice，并且不希望复用的完整 workflow 触碰 mysql/redis 等未变更组件时，可指定 `executionScope=changed_components`：

```json
{
  "version": "1.1.0",
  "strategy": "rolling",
  "executionScope": "changed_components",
  "autoExec": true,
  "workflowId": "wf-custom-update",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ]
}
```

该请求只裁剪本次 workflow task 的 deploy/默认 component jobs。若 backend 依赖的 config/secret 也必须同步，需要把对应组件作为本次变更一起提交，或使用默认 `full_workflow`。

### 9. 指定本次版本更新回调

为本次版本更新产生的 workflow task 指定独立终态回调，不修改 App 或 Workflow 默认 callback：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-custom-update",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ],
  "callback": {
    "success": "https://example.com/version/success",
    "failure": "https://example.com/version/failure",
    "timeout": "https://example.com/version/timeout",
    "reject": "https://example.com/version/reject",
    "cancelled": "https://example.com/version/cancelled",
    "methods": {
      "success": "POST"
    },
    "headers": {
      "X-Source": "eruun"
    },
    "timeoutSeconds": 30
  }
}
```

对应请求文件：`examples/version-update/11-update-with-task-callback.json`

无实际组件变更但仍需要收到回调时，可传入当前已保存的组件规格和本次 task callback；Eruun 会更新版本号、记录 update operation task，并发送 `success` 回调，不重新执行 workflow。若 update operation task 创建失败，请求会返回错误且不会发送 callback：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-custom-update",
  "autoExec": true,
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.0.0"
    }
  ],
  "callback": {
    "success": "https://example.com/version/success",
    "failure": "https://example.com/version/failure",
    "timeoutSeconds": 30
  }
}
```

### 10. 延迟执行自动工作流

为自动执行工作流设置未来执行时间：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-custom-update",
  "executeAt": 1735689600,
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ]
}
```

**响应示例：**

```json
{
  "appId": "abc123xyz",
  "version": "3.0.0",
  "previousVersion": "2.0.0",
  "strategy": "rolling",
  "taskId": "task-789xyz",
  "updatedComponents": ["backend"],
  "addedComponents": ["message-queue"],
  "removedComponents": ["deprecated-worker"]
}
```

### 11. 金丝雀发布

使用金丝雀策略更新部分副本：

```json
{
  "version": "2.1.0",
  "strategy": "canary",
  "components": [
    {
      "name": "frontend",
      "image": "myapp/frontend:v2.1.0",
      "replicas": 1
    }
  ],
  "autoExec": true
}
```

### 12. 仅更新版本号（不触发部署）

仅更新应用版本号和描述，不触发工作流：

```json
{
  "version": "2.0.0",
  "autoExec": false,
  "description": "Major version bump - documentation only"
}
```

### 13. 更新环境变量

更新组件的环境变量配置：

```json
{
  "version": "1.2.0",
  "components": [
    {
      "name": "backend",
      "env": {
        "LOG_LEVEL": "debug",
        "FEATURE_FLAG": "enabled",
        "DB_POOL_SIZE": "20"
      }
    }
  ]
}
```

### 14. 只清理全部组件资源

清理当前应用全部 DB 已知组件的 Kubernetes 资源，但保留组件 DB 记录：

```json
{
  "version": "1.1.0",
  "components": [
    {
      "action": "remove",
      "name": "cleanup_all"
    }
  ],
  "description": "Clean all component resources before manual recovery"
}
```

### 15. 只全量部署全部组件

通过选定 workflow 部署当前应用全部 DB 已知组件，不新增名为 `all` 的组件：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-deploy-all",
  "components": [
    {
      "action": "add",
      "name": "all"
    }
  ],
  "description": "Deploy all existing components"
}
```

### 16. 清理后全量部署

先清理当前应用全部 DB 已知组件资源，再通过选定 workflow 重新部署全部组件：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-deploy-all",
  "components": [
    {
      "action": "remove",
      "name": "cleanup_all"
    },
    {
      "action": "add",
      "name": "all"
    }
  ],
  "description": "Recreate all component resources"
}
```

### 17. 重启组件

不修改组件规格，只通过 workflow task 对指定组件触发 rollout restart。workflow job 在 patch 成功后会继续等待本次 restart 产生的新 Pod Ready，因此 callback success 表示重启后的 Pod 已恢复 Ready，而不是只表示 Kubernetes patch 已接受。

```json
{
  "version": "1.1.1",
  "components": [
    {
      "action": "restart",
      "name": "backend"
    }
  ],
  "description": "Restart backend without spec changes"
}
```

**响应示例：**

```json
{
  "appId": "abc123xyz",
  "version": "1.1.1",
  "previousVersion": "1.1.0",
  "strategy": "rolling",
  "taskId": "task-restart-123",
  "workflowId": "wf-default",
  "updatedComponents": [],
  "addedComponents": [],
  "removedComponents": [],
  "restartedComponents": ["backend"]
}
```

---

## cURL 命令示例

### 基本镜像更新

```bash
curl -X POST "http://localhost:8000/api/v1/applications/app-123/version" \
  -H "Content-Type: application/json" \
  -d '{
    "version": "1.1.0",
    "strategy": "rolling",
    "components": [
      {"name": "backend", "image": "myapp/backend:v1.1.0"}
    ]
  }'
```

### 新增组件

```bash
curl -X POST "http://localhost:8000/api/v1/applications/app-123/version" \
  -H "Content-Type: application/json" \
  -d '{
    "version": "2.0.0",
    "components": [
      {
        "action": "add",
        "name": "redis",
        "type": "store",
        "image": "redis:7-alpine",
        "replicas": 1
      }
    ]
  }'
```

### 重启组件

```bash
curl -X POST "http://localhost:8000/api/v1/applications/app-123/version" \
  -H "Content-Type: application/json" \
  -d '{
    "version": "1.1.1",
    "components": [
      {"action": "restart", "name": "backend"}
    ]
  }'
```

### 查看工作流执行状态

更新后可以使用返回的 `taskId` 查询执行状态：

```bash
curl "http://localhost:8000/api/v1/workflow/tasks/task-456/status"
```

### 取消延迟版本更新任务

```bash
curl -X POST "http://localhost:8000/api/v1/applications/app-123/version/cancel" \
  -H "Content-Type: application/json" \
  -d '{
    "taskId": "task-delay-123",
    "user": "admin",
    "reason": "cancel delayed rollout"
  }'
```

---

## 最佳实践

1. **生产环境使用滚动更新**：默认的 `rolling` 策略可以保证服务可用性
2. **测试环境可用重建更新**：`recreate` 策略更新速度快，适合测试环境
3. **大版本更新使用金丝雀/蓝绿**：降低风险，便于回滚
4. **设置合理的副本数**：确保更新期间有足够的副本处理流量
5. **添加更新说明**：使用 `description` 字段记录更新原因，便于审计

## 注意事项

- 组件名称会自动转换为小写
- 同一次版本更新请求不能重复 `remove`、`restart` 或 `update` 同一个组件名，也不能对同一个组件名组合互斥 action；如需替换或重建同名组件，应先完成删除清理，再发起新增/更新请求
- `autoExec: true` 删除组件会随工作流执行资源清理；`autoExec: false` 不创建工作流或 Task 清理计划，但普通 `remove` 仍会同步清理被删除组件的 Kubernetes 资源
- `remove cleanup_all` 必须创建 workflow task，不提供同步清理兜底，且 workflow cleanup 保留 standalone PVC 和五类 RBAC
- 全量清理/全量部署使用 `components[]` 保留动作表达
- `remove cleanup_all + add all` 表示全量重建，清理会在部署前执行；非共享 `store` 可在 StatefulSet `namespace/name` 不变时修改不可变 spec。必删重建会先把 VCT retention 收敛为 `Retain/Retain`，主动清除并复验匹配 PVC 的 StatefulSet/ordinal Pod owner reference，再以 `Orphan` 删除 StatefulSet 并逐 Pod 重检 share 标签：cleanup v2 保留全部 VCT PVC，cleanup v3 只删除受影响模板的 PVC 数据并等待其消失；受 `share.strategy=default` / `ignore` 保护的组件仍拒绝此类修改。如果 workflow 有前置审批步骤，则审批仍位于最前，审批通过后再清理；滚动发布须先升级全部 worker 再开放可能生成 v2/v3 的请求
- `restart` 只支持已有 `webservice` / `store` 组件，写入 `kubectl.kubernetes.io/restartedAt` 注解并等待本次注解对应的新 Pod Ready；stopped、`share=default`、`share=ignore`、未知 share 策略或集群资源不存在时会在 workflow job 中记录 skipped，`share=force` 仍按普通组件执行
- `restart` 不修改组件规格，不能与同组件的 `update` / `add` / `remove` 或全量清理/部署保留动作混用
- `autoExec: true` 且有组件变更或资源动作时，必须找到并成功提交可执行工作流；无可执行工作流或提交失败会返回错误
- `autoExec: true` 时如果目标工作流 `steps` 为空，会在提交版本更新前拒绝请求，不会改写应用版本、组件或任务记录
- 即时 `autoExec: true` 创建真实 workflow task 后，APP 聚合状态会优先显示 `updating` 直到该 task 进入终态；`failed`、`restarting`、`starting`、`cleaning` 等更高或更具体状态保持原样
- `callback` 只覆盖本次版本更新 task；`autoExec: false` 时会被忽略；无实际组件变更时会触发一次 update operation task 的 `success` 回调但不执行 workflow；显式 task callback 的 task 创建失败会返回错误且不发送 callback
- 延迟执行（`executeAt` 为未来时间）不会提前把组件状态标记为 `Updating`
- 延迟删除组件会预创建资源清理记录；在该清理任务执行、取消或进入终态前，应用会被视为有待处理工作流，后续版本更新不能复用同名组件
- 可通过 `POST /api/v1/applications/:appID/version/cancel` 取消尚未执行的延迟任务；取消与调度器原子竞争，已执行或不可取消时返回 409 / 业务码 10033，且不会覆盖已开始执行的任务状态
- 新增组件时必须指定 `type` 字段
