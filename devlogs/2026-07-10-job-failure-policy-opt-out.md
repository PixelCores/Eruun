# Job Failure Policy Opt-out

## 背景与需求

Workflow 的 `failurePolicy` 是整条 workflow 的统一策略，无法表达“普通组件失败执行 `cleanup_all`，临时 SQL Job 失败只清理该 Job”。复用 `properties.runPolicy=recreate` 会混淆同名 Job 重建策略和失败清理范围，因此需要一个独立且最小的 Job 级例外。

## 影响范围

- API: 顶层 `type=job` 组件新增可选 `properties.failurePolicy=cleanup_failed`；Try 校验新增 `INVALID_JOB_FAILURE_POLICY`。
- Domain/DB: 字段复用现有 `WorkflowFailurePolicy` 类型并保存在 Component `properties` JSON，不新增表或列。
- Cache/K8s: 无缓存结构变化，不新增 Kubernetes annotation。
- Workflow: Job 主任务可以覆盖 workflow 策略；附属资源任务继续继承 workflow 策略。

## 技术选型与取舍

- 复用字段名和现有 `cleanup_failed` 值，不新增布尔开关或另一套 cleanup 枚举。
- Job 层只允许 `cleanup_failed`，空值表示继承；不开放反向 `cleanup_all` 覆盖，保持本次能力最小化。
- override 只写入主 `instant_job` 的 transient `JobTask`，不写数据库新列或 K8s annotation，也不扩散到 PVC/RBAC 等附属任务。
- 并行任务按“任一失败任务需要 `cleanup_all` 即全量清理”决策，避免 SQL Job opt-out 掩盖同批次普通组件失败。

## 实现摘要

- 组件 Properties 增加可选 `failurePolicy`，创建和 `/version` add/update 写入路径统一规范化并 fail-fast 校验。
- Properties 内部使用可选指针区分省略与显式空值；Job 空值在写入前规范化为省略，非 Job 显式设置（包括空值）直接拒绝。
- `/version update` 保持既有 Properties 全量替换契约：省略整个 `properties` 时保留现值；携带 `properties` 时，省略或清空 `failurePolicy` 都会清除已有 Job override。
- 模板 Job 请求省略字段时保留模板值，显式空值清除 override，`cleanup_failed` 覆盖模板值；请求侧 nested init 在模板展开前校验，避免静默丢弃。
- Try Application 对非法值、非 Job 类型和 init container 误用返回字段级错误；模板展开后的 override 错误仍定位到原始请求 `component[i]`，不使用克隆结果下标。
- Job Builder 仅给主 Instant Job 任务携带 override；Controller 以任务 override 优先、workflow 策略兜底决定清理范围。
- 并行混合失败仍以首个失败任务作为主原因；若另一个任务触发 `cleanup_all`，终态、返回错误和 callback `reason` 会追加该触发任务。
- `runJob -> InstantJobCtl.Clean` 继续负责局部删除本次执行创建的 Kubernetes Job，workflow 终态仍为失败。

## 测试与验收

执行 config、validation、application、workflow 定向测试，并执行全仓 race/coverage、vet 与 diff 检查。验收场景包括：

- `runPolicy=recreate` 与 `failurePolicy=cleanup_failed` 可同时持久化且语义独立。
- SQL Job 单独失败/超时不触发 `cleanup_all`；普通组件失败仍触发。
- SQL Job 与普通组件并行失败时仍执行 `cleanup_all`。
- `cleanup_all`、未知值、非 Job 类型和 init container 使用均被拒绝。

## 风险与后续

- 延迟到未来执行的 Job 当前在 workflow 中以 `distributed` 视为已分发成功，其异步终态不触发 workflow 失败清理；本次不改变该既有边界。
- `scheduledjob` 和 `cloudjob` 不在本次 Job 级例外范围内。
