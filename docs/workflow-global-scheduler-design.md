# Eruun Job 全局调度与失败策略

> 状态：Implemented Reference。本文描述已实现的 Job 级准入与 OOM 失败策略，保留原 Workflow 执行与 ownership 边界。

## 1. 适用性结论

原设计的数据库事实源、generation/token fencing、Worker heartbeat 和 Scheduler Leader 适用 Eruun，继续使用。原先只以 Workflow Run 为准入单位，不能满足跨 Workflow 排列每一个 Job：长 Workflow 会持续占用执行机会，内部 ready Job 无法与其他 Workflow 竞争。因此本实现把**执行机会的选择下沉到依赖已就绪的 Eruun JobTask**，保留 Workflow 的调度、步骤、审批、取消和恢复 ownership。

不新增 Scheduler 服务、CRD、顶层队列表或另一套执行租约。既有 `JobInfo` 同时保存 Job 执行记录和调度状态；既有 `SystemSetting` 的 `workflow_scheduler` 行保存全局策略，并作为准入事务的串行锁。当前 `scheduler` 角色负责放行，`worker` 负责提交 ready Job、等待准入、执行和释放；API 处理审批取消或拒绝产生的终态回调，也登记并等待同一准入。

## 2. 开源策略取舍

| 参考 | 采用的思想 | Eruun 的选择 |
| --- | --- | --- |
| [Kueue ClusterQueue](https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/) | 优先级、FIFO、准入；跳过暂时不能准入的工作 | 空间达到并发上限时可以选择其他空间的 Job，避免队头阻塞 |
| [Kueue WorkloadPriorityClass](https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/) | 业务执行优先级独立于 Pod 放置优先级 | `schedulingClass` 控制 Eruun 的执行机会，不映射为 Kubernetes 抢占 |
| [Volcano Scheduler](https://volcano.sh/docs/scheduler/overview/) | 对 eligible task 分阶段排队和分配 | 保留当前依赖就绪门禁；不引入节点绑定、gang、插件链和另一套资源对象 |
| [Kubernetes non-preempting priority](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#non-preempting-priorityclass) | 高优先级先排队且不驱逐正在执行的工作 | 当前调度非抢占，已获准执行的 Job 可以完成 |
| [Kubernetes Pod failure policy](https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-failure-policy) | 按明确的失败原因决定重试或结束 | OOM 以本次 Job 所属 Pod 的容器终止原因识别；重试预算和资源调整由 Eruun 的持久 checkpoint 控制 |

以上是策略参考，并不声明 Eruun 实现了 Kueue 的资源公平共享或 Volcano 的 gang scheduling。Eruun 还执行 ConfigMap、PVC、Deployment、清理、回调和资源导入等控制操作，整体引入面向 Pod 的批处理调度器会增加一套队列及 ownership，不能统一覆盖这些工作。

## 3. 调度范围与标记

Workflow 中每个 component step 可写 `schedulingClass`，subStep 可覆盖父 step；未指定使用 `normal`。字段随现有 Workflow JSON 保存、查询和生成传播。一个 component 生成的 Service、ConfigMap、PVC、workload 等 Job 都继承该标记。

```json
{
  "name": "deploy",
  "workflow": [{
    "name": "services",
    "mode": "DAG",
    "schedulingClass": "high",
    "subSteps": [
      {"name": "api", "jobType": "deploy", "components": ["api"]},
      {"name": "batch", "jobType": "deploy", "components": ["batch"], "schedulingClass": "background"}
    ]
  }]
}
```

| class | 基础分值 |
| --- | ---: |
| `background` | 0 |
| `normal`（默认） | 50 |
| `high` | 100 |

三类取值固定，未知值拒绝；不新增优先级类管理实体。当前调用方可以在自己的空间中选择任一类，空间并发上限独立于优先级。

原 `JobPriority*` bucket 表达资源依赖顺序，保持不变。Job 只有在前置步骤、审批、资源 bucket 和 Worker 本地并发条件允许执行时才进入全局 ready 队列。优先级不能把后置 Job 提到依赖之前，也不替换当前 StepByStep/DAG 语义。队列排序覆盖已经登记的 ready Job；尚未由 Worker 接管的 Workflow 或尚未到达的步骤没有提前占用 Job 槽位。

覆盖范围：普通生成 Job、失败清理、审批通知、终态 callback、资源扫描/纳管、到期 delayed Job 的实际分发。审批通知继承审批 step 的 class；没有用户标记的内部 Job 使用 `normal`。Kubernetes CronJob 创建的后续 batch Job/Pod 属于 Kubernetes 控制器，不是新的 Eruun JobTask。

## 4. 全局策略与选择规则

管理员通过现有系统设置 API 管理 `workflow_scheduler`，无需增加配置接口或每个 Worker 独立配置。启动时仅在缺少该行时初始化默认值，已有值必须合法。该行不能通过系统设置 API 删除。

```json
{
  "strategy": "priority",
  "maxConcurrentJobs": 100,
  "maxConcurrentJobsPerWorkspace": 10,
  "agingSeconds": 60
}
```

- `strategy`: `priority` 或 `fifo`。
- 全局并发：1..10000；空间并发：1..全局并发。
- `agingSeconds`: 1..86400。默认值是明确的保守初值，运维应按 Worker 数量和控制面延迟调整。
- `priority`: 有效分值 = 基础分值 + floor(数据库等待秒数 / agingSeconds)。分值不封顶，较老的低优先级 Job 可以超过新到的高优先级 Job。
- 有效分值相同，优先选择当前活动 Job 较少的空间；再按首次入队时间和 Job ID 排 FIFO。这是简单的并发公平规则，不承诺按运行时间或资源消耗加权分配。
- `fifo`: 按首次入队时间、Job ID 选择；仍跳过达到空间上限的 Job。
- 两种策略都遵守全局与空间并发上限；每轮最多新放行 100 个 Job。扫描分页，父 Workflow 在同轮复用已读状态。

降低上限不终止已有 Job，只阻止后续超额准入。Job 结束或失去 ownership 后释放逻辑槽位。同一调度 generation/status 内的队列重入保留等待时间，不能通过反复消息投递刷新 FIFO 或逃避老化。

**槽位约束的是 Eruun Job 控制器执行并发，不是存量 Pod 的 CPU/内存配额。** Deployment 就绪、CronJob 配置完成或 delayed Job 分发结束后，它们创建的 Kubernetes 资源可以继续存在。Pod 的节点选择、资源可满足性和 ResourceQuota 仍由 Kubernetes 决定。

## 5. 数据库、恢复和取消

`JobInfo` 的调度状态为 `queued → admitted → released`，独立于现有业务 `status`。记录 class、基础优先级、首次入队时间、调度原因、当前 Workflow owner generation/status，以及有界 detached 操作的 deadline。沿用既有唯一 `execution_key`，不复制 Job payload 到第二张表。

准入事务先锁定系统策略行，然后在 READ COMMITTED 下读取有效活动数、选择候选和写入 admitted。这样两个 Scheduler 即使在任期切换附近并发运行，也必须顺序重新计算剩余容量。MySQL 使用 matched-row CAS 语义，不能把同毫秒相同值写入产生的零 changed rows 当作 ownership 丢失。

普通 Job 的入队、准入确认、释放遵守父 Workflow generation/token/worker/status fencing；新准入及执行确认要求有效 running lease。已获准执行的槽位，在同代父 Workflow 仍为 running 时不会只因心跳短暂过期而释放，须等待现有 reaper 撤销 ownership，避免原 Worker 续租后与新 Job 同时占用一个槽位。Callback 可以在 `wait_for_approval` 或终态执行，使用既有 callback timeout 作为有界 deadline。Worker 回调匹配该代 ownership；服务层审批取消/拒绝/超时回调也经过相同准入，即使父任务从未被 Worker 接管，仍按真实父记录的终态、generation、token 和 worker 精确 CAS，不能用空身份绕过。两条终态回调路径共用同一执行键。

Delayed Job 只有在数据库中存在到期且 pending 的 checkpoint 时才可独立进入队列。它不再依赖已经结束的父 Workflow lease，使用 checkpoint identity 和短期限准入；排队等待沿用已有 dispatcher 轮询，不消耗基础设施失败的指数退避次数。过期 deadline 的重新准入不能被旧 dispatcher 释放。

Worker 等待准入时响应取消与基础设施停止；失去 ownership 后不执行资源操作。Scheduler 会回收已终止、过期或不再属于该代的调度记录。此恢复沿用既有 at-least-once/Kubernetes identity 幂等语义，不承诺外部操作 exactly-once。

## 6. OOM 失败策略

现有 Workflow `failurePolicy=cleanup_failed|cleanup_all` 仍决定**最终失败后的清理**。新的 component `properties.jobRetryPolicy` 决定立即执行的 `job` 组件在 OOM 后停止、原资源重试或扩容重试：

```json
{
  "onOOM": "resize",
  "maxRetries": 2,
  "backoffSeconds": 5,
  "memoryGrowthFactor": 2,
  "cpuGrowthFactor": 2,
  "maxResources": {"memory": "2Gi", "cpu": "2"}
}
```

- 未配置时保留已有 Kubernetes Job 行为；显式 `{"onOOM":"stop"}` 将 OOM 明确终止。
- `retry` 要求 `maxRetries` 1..10、`backoffSeconds` 1..3600，资源保持不变。
- `resize` 另要求 `memoryGrowthFactor` 为 2..4 和 `maxResources.memory`；只增长发生 OOM 的容器的 memory requests/limits。
- `cpuGrowthFactor` 省略、0 或 1 时 CPU 保持不变；显式设为 2..4 才同步增长 CPU requests/limits，且必须提供 CPU 上限。CPU limit 超额通常触发 throttling，OOM 本身不是 CPU 不足的证据，见 [Kubernetes 资源说明](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits)。
- requests/limits 必须有效，增长不能溢出或突破绝对上限；达到上限、耗尽次数、非 OOM、取消或总 deadline 到达后停止。
- OOM 判断匹配当前 Job UID/controller owner 和容器的本次 `terminated.reason=OOMKilled`；单独 exit code 137、旧 Pod、`lastTerminationState` 不构成扩容依据。
- 仅支持同步立即执行的 `job` 组件；scheduled、未来 `startTime`、不适用组件或嵌套容器上的策略显式拒绝，不能静默忽略。

显式策略使用 `backoffLimit=0` 和 `restartPolicy=Never`，避免 Kubernetes 与 Eruun 重复计算业务重试。Eruun 在 `JobInfo.InternalInfo` 中保存 attempt、资源快照、旧/当前 UID、退避时间和总 deadline，再删除匹配 UID 的失败 Job，等待旧 Job/Pod 停止后创建下一次尝试。重试期间保留同一逻辑调度槽位，总执行超时包含退避，不为每次尝试重置。

Workflow lease 恢复会读取已持久的运行中 retry checkpoint，保留原 execution identity、次数、资源与 deadline；不能因为新 generation 而重置预算或再次增长。最终失败才调用既有 Job/Workflow 清理。成功时先采集日志、持久化结果，再删除当前 UID；保存失败保留证据供恢复。创建前设置的 Job TTL 覆盖剩余 deadline 加原有一小时清理余量，确保恢复期限内不提前删除证据，并在保存后进程退出或清理失败时最终回收资源。历史无 TTL 的检查点恢复时补齐期限。同一 lease 的父工作流已取消时，控制器无需等待取消信号即可停止，并清理对应 UID；新 lease 或其他 UID 仍被隔离。

## 7. 可观测性和交付边界

现有 task stages 查询的 `info` 展示每个已登记 Job 的调度状态、class、首次排队时间与原因。Job 记录保存 attempt 和最终错误；结构化日志说明 OOM 决策与调度错误。Workspace 身份不作为无界 Prometheus label。

升级需要先完成 schema migration，再统一升级四角色；新增调度列使用可空字段或明确的零值默认值，不改旧 Job 业务状态。回滚前应停止接收新任务并排空已启用 retry policy 的运行中 Job；旧 Worker 不理解新的准入与 checkpoint，不能混跑并声称全局上限有效。

本实现不包括 GPU/设备容量预留、节点放置、按资源量的配额、gang scheduling、checkpoint 抢占或任务 deadline 公共 API。需要这些能力时再基于明确负载与 Kubernetes 侧资源事实设计，不能把本次逻辑并发槽位解释为这些功能。

验收应覆盖：请求/查询标记、依赖不变、跨 Workflow 顺序与空间上限、老化、重复投递、并发 Scheduler、MySQL matched-row/事务行为、ownership 变化、callback/delay 路径、OOM 识别/资源上限/持久化失败/恢复预算、格式/vet/race/build、部署及既有回归。
