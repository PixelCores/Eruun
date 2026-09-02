# StatefulSet Immutable Rebuild Safety

## 背景与需求

版本更新允许通过 `remove cleanup_all + add all` 显式重建非共享 store StatefulSet，但原契约存在多处安全缺口：真实 Kubernetes 在 `whenDeleted=Delete` 时会随 StatefulSet 删除全部 VCT PVC；失败的 v2/v3 清理可能被普通部署或数据库重置绕过；cleanup Job 完成但后续部署失败时可能过早解除 pending；并发版本更新与 workflow 入队可能同时通过空闲检查。fake client 不模拟 StatefulSet PVC GC，因此单看原单元测试无法发现第一类数据丢失。

## 影响范围

- API：版本更新、直接 workflow、定时调度、数据库重置、既有应用整体刷新与公共资源清理共享应用级锁和 StatefulSet cleanup fence；锁冲突/后端不可用分别返回 10031/10032，pending fence 返回 10000。
- Domain：cleanup v2/v3 的失败状态形成持久化部署栅栏，并可由显式全量重建恢复。
- Kubernetes：必删 StatefulSet 在删除前强制使用 `Retain/Retain`，v3 只显式删除计划内 VCT PVC。
- Workflow：v2 的 Pod/owner Job share 保护与 v3 一致；普通无不可变变化的全量重建继续发出 cleanup v1。
- DB：不新增 schema；task `cleanup_info` 增加可选 `resolvesTaskIDs` 因果引用，预创建 `JobInfo.internal_info` 同时承载 cleanup marker 与必删 StatefulSet 的 Pod 身份 checkpoint。

## 技术选型与取舍

- 选择复用现有 app schedule 分布式锁，覆盖版本更新完整读写、direct/cron enqueue、数据库重置、既有应用整体刷新和公共资源清理，并为这些长临界区启用自动续期，避免新增第二套互斥实体和锁顺序。既有应用刷新和公共清理先从数据库解析 canonical app ID，锁键统一 trim 并小写化，避免 MySQL 大小写不敏感查询命中同一应用时使用两把 Redis 锁。
- 不使用各 API 实例本地生成的 `create_time` 推断恢复先后。恢复 task 在同一应用锁和事务内把当前未完成 task ID 写入 `resolvesTaskIDs`；loader 先验证全部 task 合同，再只让整体成功且覆盖完整的显式引用消解旧失败，未知引用、自引用、循环和覆盖不完整都 fail-closed。
- 不依赖先删 StatefulSet 再由 Kubernetes retention 决定 PVC 命运。worker 先更新 `whenDeleted`、`whenScaled` 为 `Retain`，等待 controller 观察新 generation，再主动移除并复验全部精确匹配 VCT PVC 上指向当前 StatefulSet 或其 ordinal Pod 的 owner reference；这也覆盖已经缩容、当前没有 Pod 的高 ordinal PVC。无法确认时超时失败。
- Pod 预检把目标 StatefulSet 是否存在及 UID 固定到 checkpoint，retention 从该身份初始化并在每次 GET 后交叉校验；因此预检到首次 retention poll 之间的同名 replacement、以及原本不存在却新出现的对象也会立即冲突。v2 的全部 VCT PVC 和 v3 未计划删除的 VCT PVC 只要已经进入 Terminating 就阻止继续，避免删除 Pod 后 `pvc-protection` 解除造成静默丢卷。
- 必删 StatefulSet 使用 `Orphan` propagation，避免 Pod 在第二次 live share 检查前被 Kubernetes 级联删除；Orphan 前按固定 StatefulSet UID 查找 Pod，并把 Pod name/UID 与已验证的 owner Job name/UID 身份一并 checkpoint 到当前 JobInfo。worker 先收敛并确认 owner Job 消失，再按固定 UID 逐个重读残留 Pod 的 share 标签；即使 Pod 已进入 Terminating 也不会跳过 owner Job，同 task 重启时 Pod 已消失仍可继续按 checkpoint 收敛，新 task 或旧的不完整 checkpoint 无法证明身份时 fail-closed。
- 不只依赖 Redis cancel signal。retention/owner reference、checkpoint、StatefulSet、owner Job、Pod 和计划 PVC 的每次 mutation 前都重读持久化 WorkflowQueue；checkpoint 持久化和 Pod 清理阶段还校验 JobInfo 为 running。task 已终态时立即停止，context cancellation/deadline 也会优先映射为 cancelled/timeout；当前 worker 会将仍为 running 的 JobInfo 写入相应终态，避免取消已提交但信号发布失败时继续清理、被误记为 failed 或永久停在 cancelling。
- v2 保留全部 VCT PVC；v3 只删除持久化计划中的模板。将契约改为全量删除会扩大数据损失，也会绕过 share 保护。
- v3 pending 恢复必须让 historical descriptor 到本次 desired StatefulSet 的 VCT transition 覆盖每个尚未完成的模板；只修改另一个 VCT 不能借机把已经恢复为旧规格的 PVC 继续带入删除计划。
- 只在请求确实产生不可变 StatefulSet 变化时升级 cleanup v2/v3。这样普通全量重建维持 v1 滚动兼容；显式不可变迁移仍要求 worker-first 发布，因为旧 worker 必须对未知契约 fail-closed。

## 实现摘要

- UpdateVersion 在应用锁内完成 pending/idle 检查、组件与应用写入、task/JobInfo 创建。
- workflow direct、cron 与 database reset 在相同锁内检查持久化 v2/v3 fence；只有 `/version` 的合法恢复请求可创建下一次迁移 task。
- `POST /applications` 和 `POST /applications/create-and-exec` 仅在命中既有 ID/template key 时获取同一应用锁：锁内先做 idle/pending fail-fast，刷新事务内重读应用并再检查一次，然后才替换组件和 workflow。全新创建和 template miss 不解析 Redis locker。
- `DELETE /applications/:appID/resources` 在同一锁内重读应用并在任何状态/Kubernetes/task 写入前检查 idle/pending。级联删除整个应用是显式终止操作，它已持有同一锁并取消活跃任务，因此通过内部路径有意跳过恢复 fence，避免嵌套加锁。
- 已提前创建的普通延迟 workflow 在到点从 `waiting` 进入 `queued` 前，会在同一应用锁内再次检查 fence；迁移/恢复 task 自身豁免，立即任务保持原分发路径。
- pending loader 同时回放 v2/v3 历史，使用显式 `resolvesTaskIDs` 构建与时间戳/查询顺序无关的恢复关系。只有 cleanup JobInfo 与整体 WorkflowQueue task 都为 `completed`，且恢复 task 完整覆盖它引用的旧契约，才解除 pending；cleanup 已完成但后续 deploy 失败、`passed` / `skipped` 或其他失败终态都会保留 pending。父 task 已进入可恢复终态、但预创建 cleanup JobInfo 仍为空、`created`、`queued`、`waiting` 或 `pending` 时，按“尚未执行”的 pending 契约处理；`prepare`、`running` 等真正活跃状态仍阻止恢复。
- cleanup worker 在任何 StatefulSet 删除前完成全量目标 share 预检、固定 StatefulSet/Pod UID、持久化 Pod 身份、retention 更新、保留 PVC 的 Terminating 检查与 owner reference 主动移除/复验；retention mutation 和精确 StatefulSet Delete 前刷新 live share 与完整 target set，每次 mutation 前复验 WorkflowQueue 仍可执行，并在 checkpoint/Pod 阶段复验 JobInfo。安全刷新对 429、timeout 与 5xx 做有限重试，每次重试先重读 WorkflowQueue；身份/share/目标冲突立即 fail-closed。以 `Orphan` 删除 StatefulSet 后，已 checkpoint 的 owner Job 会从通用 generated/labeled 删除路径中排除，只由专用收敛器处理；owner Job 和 Pod 都用 live UID/resourceVersion precondition 显式删除，owner Job 同样使用 `Orphan` 防止 GC 越过 Pod 复查，然后才执行 v3 计划 PVC 删除。
- StatefulSet、owner Job、Pod 与计划 PVC 的 Delete 若仅因控制器正常更新导致 resourceVersion 冲突，会重新读取 live 对象并复验 UID/share：同 UID且未受保护时使用最新 resourceVersion 有界重试，UID replacement 或新增 `default` / `ignore` 保护仍立即 fail-closed。
- fake client 测试通过 reactor 模拟 controller generation 观测和不安全删除时的 PVC GC，并验证 worker 主动清除缩容残留 PVC owner reference。

## 测试与验收

验证命令：

```bash
go test -count=1 -race ./pkg/apiserver/event/workflow/job
go test -count=1 -race ./pkg/apiserver/domain/service/application
go test -count=1 -race ./pkg/apiserver/domain/service/workflow
go test -count=1 -race ./pkg/apiserver/interfaces/api
go test ./... -race -cover
go vet ./...
git diff --check
```

验收口径：v2/v3 都必须在 Retain generation 被观察且匹配 PVC owner reference 清除前拒绝删除 StatefulSet；首次 StatefulSet UID 不得被同名 replacement 覆盖，任一应保留 VCT PVC 已进入 Terminating 都必须停止；retention/delete 前新增 share 保护或额外 StatefulSet 必须在后续 mutation 前停止；必删 StatefulSet 与 owner Job 使用 `Orphan`，并以持久化的 Pod name/UID 在显式删除前重检 share，Pod/Job 删除都携带 live resourceVersion；同 UID且未保护的 benign resourceVersion 漂移必须继续收敛，replacement 或新增 share 必须停止；持久化 task 已取消时即使 JobInfo 仍 running、取消信号未送达也必须零 Kubernetes mutation，并将 JobInfo 收敛到终态；v2 不删除 VCT PVC，v3 只删除计划模板且 pending 恢复必须覆盖原模板；受保护 StatefulSet/Pod/owner Job 阻止两种必删契约；只有 cleanup Job 与整体 Workflow 都完成、且显式因果引用完整覆盖旧契约时才解除 fence；失败契约阻止普通部署、数据库重置、既有应用刷新、公共资源清理和既有延迟任务到点分发，但允许严格恢复与显式级联删除；并发应用变更最多一个进入临界区。

## 风险与后续

- fake client reactor 覆盖了 Kubernetes GC 的关键语义，但仍建议在真实集群集成环境验证不同 Kubernetes 版本中 retention generation 观测、PVC ownerReference 更新与 `Orphan` 删除的组合行为。
- cleanup v2/v3 是 worker 能力边界；滚动发布必须先升级全部 worker，再开放可能生成 v2/v3 的显式不可变迁移流量。普通 cleanup v1 全量重建不受该前置影响。
- `resolvesTaskIDs` 是 cleanup_info 内的可选字段，旧 worker 会忽略；为避免旧 API 实例继续创建无显式因果引用的恢复 task，发布时还应排空旧 API 实例。历史无引用 task 会安全地保持 pending，并可在升级后通过一次新的显式恢复消解。
