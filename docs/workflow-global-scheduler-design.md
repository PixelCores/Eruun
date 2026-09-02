# Eruun Workflow 全局调度层设计

> 状态：Draft / Proposal。本文定义面向所有 Eruun Workflow Run 的统一调度层，不代表当前 Dispatcher 已具备优先级、公平性、项目配额或抢占能力。

## 1. 文档关系

- [企业级分布式运行时设计](enterprise-distributed-runtime-design.md) 定义 Scheduler 角色、数据库租约、Worker 心跳和 fencing；本文以这些能力为前提。
- [Agent 评测 Job 设计](agent-evaluation-job-design.md) 定义可协作式抢占的评测执行单元和 checkpoint 协议。
- [Workflow 资源补偿型 Scheduler 技术方案](workflow-resource-capacity-scheduler.md) 处理部署前的 CPU、内存、GPU 容量准入与扩容，属于后续 admission plugin，不是本文的全局队列调度器。
- 当前链路与状态事实以 [Workflow 架构指南](workflow-architecture-guide.md) 和代码为准。

## 2. 设计决策

全局调度单位是 **Workflow Run**，由 `WorkflowQueue.taskId` 唯一标识。Workflow 内部的 Step、Job 优先级和并发继续由 WorkflowCtl 管理，不把每个 Deployment Job、CloudJob 或 Agent Eval Runner 复制到另一个顶层队列。

```text
API/Domain creates WorkflowQueue(status=waiting)
                 |
                 v
        Global Scheduler leader
  priority + project fairness + quotas
                 |
        reserve capacity by DB CAS
                 v
       WorkflowQueue(status=queued)
                 |
           Redis/Kafka dispatch
                 v
               Worker
                 |
       WorkflowCtl -> Steps -> Jobs
```

该决策保证：

- 部署、更新、清理、数据库重置和 Agent 评测共用一个排队事实源。
- 原有 Workflow 的步骤顺序、审批、失败清理和 callback 语义保持不变。
- `JobInfo` 继续是执行记录，不被误用为新的全局待调度实体。
- 调度策略可以演进，而不改变具体 Kubernetes 资源控制器。

## 3. 当前能力与缺口

当前 Dispatcher 周期读取 `waiting` 任务，以 CAS 更新为 `queued` 后立即投递到消息队列；Worker 全局并发由每个进程内的 semaphore 控制。Workflow 内部已经有 Job priority bucket，但它只决定一个 Workflow Step 中资源的执行顺序。现有运行时已经通过 `runGeneration`、`runToken`、`workerId`、`heartbeatAt`、`leaseExpiresAt`、`dispatchAttempts` 和 `schedulingReason` 实现数据库执行租约、Worker 续租与 lease reaper 恢复，本方案直接复用这些事实字段和恢复能力。

当前没有以下全局能力：

- 不同 Workflow Run 之间的业务优先级。
- 按 `projectId` 的公平性、权重和并发配额。
- 全局 100 并发的持久化 slot 预留。
- 排队 deadline、老化提升、调度原因和派发失败上限。
- Agent 评测的 checkpoint-aware 协作式抢占。

## 4. 数据模型

### 4.1 复用 WorkflowQueue

`WorkflowQueue` 继续保存 `taskId`、`projectId`、`appId`、`workflowId`、`type`、`status`、`currentStep`、审批状态和 callback。以下租约字段已经存在，不属于本方案的 additive migration；实现必须沿用现有列和类型：

| 既有字段 | 当前类型 | 复用语义 |
| --- | --- | --- |
| `runGeneration` | bigint unsigned | 单调递增的执行代次 |
| `runToken` | varchar(64) | 当前代 fencing token |
| `workerId` | varchar(255), nullable | 当前执行 Worker |
| `heartbeatAt` | datetime(6), nullable | 最近一次 Worker 心跳 |
| `leaseExpiresAt` | datetime(6), nullable | 当前数据库租约到期时间 |
| `dispatchAttempts` | int unsigned | 当前代派发尝试次数 |
| `schedulingReason` | varchar(255) | 当前等待、派发或恢复原因 |

在这些既有字段之上，以 additive migration 增加全局调度专用字段：

| 字段 | 类型建议 | 说明 |
| --- | --- | --- |
| `spec` | JSON | 不同 WorkflowTaskType 的版本化业务输入；Scheduler 不解释其业务字段 |
| `schedulingSpec` | JSON | 用户可配置调度参数的版本化快照 |
| `priority` | smallint | 内部优先级数值，默认 50 |
| `queueEnteredAt` | datetime(6) | 首次进入或重新进入 `waiting` 的时间 |
| `deadlineAt` | datetime(6), nullable | 任务排队和执行的绝对 deadline |
| `scheduledAt` | datetime(6), nullable | 最近一次成功预留 slot 的时间 |
| `preemptionCount` | int unsigned | 累计成功抢占次数 |
| `preemptible` | boolean | 是否允许协作式抢占 |

`schedulingSpec` v1：

```json
{
  "version": 1,
  "priority": "normal",
  "deadlineAt": "2026-08-27T12:00:00Z",
  "preemptible": false
}
```

建议索引：

```text
(status, priority DESC, queue_entered_at, task_id)
(project_id, status, queue_entered_at)
(status, lease_expires_at)
(status, deadline_at)
```

不创建 `SchedulerJob`、`SchedulerQueue` 或第二份任务表。调度统计从 `WorkflowQueue` 和 Worker capacity snapshot 聚合；策略存入现有 SystemSetting。

### 4.2 优先级

公开 API 使用枚举，数据库保存固定数值：

| API 值 | 内部值 | 使用者 |
| --- | --- | --- |
| `critical` | 100 | 仅系统恢复和平台紧急任务，普通用户不可设置 |
| `high` | 75 | 用户高优先级任务 |
| `normal` | 50 | 部署等现有 Workflow 的默认值 |
| `low` | 25 | Agent 评测默认值和后台批处理 |

鉴权层拒绝普通用户提交 `critical`。旧任务或空值迁移为 `normal`；Agent Evaluation 创建时显式写入 `low`。

## 5. 状态机与所有权

### 5.1 主状态机

```text
created -> waiting -> queued -> running -> completed
                    |          |         -> failed
                    |          |         -> timeout
                    |          |         -> cancelled
                    |          +-> waiting (lease/dispatch recovery)
                    +-> timeout (queue timeout/deadline)

running(agent eval only) -> preempting -> waiting
                                      \-> running (preemption aborted)
```

- `waiting`：已持久化但未占用执行 slot。
- `queued`：Scheduler 已预留全局和项目 slot，消息待投递或 Worker 待认领。
- `running`：Worker 使用 generation/token/workerId 持有数据库租约。
- `preempting`：只用于 Agent 评测；原 Worker 继续持有租约，直到 checkpoint 验证成功并主动退出。
- 审批步骤继续使用现有 `wait_for_approval`/`approvalPending` 语义；等待审批期间释放全局和项目 slot，批准后重新进入 `waiting`。
- 所有终态都释放 slot；用户取消保持 `cancelSource=user`，基础设施恢复不得写成用户取消。

Scheduler 在同一数据库事务内完成：检查 slot、选择任务、将 `waiting` CAS 为 `queued`、增加 `runGeneration`、生成 `runToken`、写入 `scheduledAt`。消息发送失败时保留 `queued` 预留，由派发重试器根据数据库记录重发；超过上限才进入明确的基础设施失败。

Worker 认领和故障恢复遵守 [企业级分布式运行时设计](enterprise-distributed-runtime-design.md) 的 10 秒 heartbeat、30 秒 lease、10 秒 reaper 和 60 秒 RTO。所有写入必须携带 generation/token fencing 条件。

### 5.2 Deadline 与派发重试

- 没有显式 `deadlineAt` 的任务最多排队 24 小时；超时后进入 `timeout`，原因 `scheduler_queue_timeout`。
- 有 `deadlineAt` 时以更早者为准；Scheduler 不启动已经无法在 deadline 前开始的任务。
- 消息派发使用指数退避，单个 generation 最多 5 次；仍失败时进入 `failed`，原因 `scheduler_dispatch_exhausted`，并保留审计记录供人工重试。
- Worker 基础设施故障导致的重新调度不消耗业务 Job 的 retry count；新 generation 的派发计数重新从 0 开始。

## 6. 公平调度算法

### 6.1 容量和配额

v1 默认策略：

- 全局并发 Workflow slot：100。
- 每个 `projectId` 默认并发：10。
- 每个项目默认权重：1。
- `queued + running + preempting` 都占用 slot；`waiting` 和审批等待不占用。
- 空 `projectId` 的历史任务归入 `_legacy` 项目，使用默认配额，不绕过公平性。

项目 override 可以调整 `maxConcurrency` 和 `weight`，但不得使项目配额超过全局并发。API/Domain 新建任务时要求有效 `projectId`；仅迁移期允许 `_legacy`。

### 6.2 Priority + Weighted Deficit Round Robin

Scheduler Leader 默认每秒执行一次调度循环：

1. 读取全局及各项目已占用 slot，计算剩余容量。
2. 对 `waiting` 任务计算 effective priority。
3. 从最高 effective priority band 开始，仅选择未达到项目配额的项目。
4. 在同一 band 中对项目执行 Weighted Deficit Round Robin；每轮增加项目 `weight` 对应的 deficit，每调度一个 Workflow 消耗 1。
5. 项目内部按 `queueEnteredAt ASC, taskId ASC` 保持 FIFO。
6. 每次选择都以数据库事务和 CAS 重新验证状态及配额，直到无空闲 slot 或没有合格任务。

deficit 和 round-robin cursor 是 Scheduler Leader 的可重建内存状态，不作为任务事实。Leader 切换后从“拥有最老合格任务”的项目开始新一轮；DB 配额校验保证正确性，短期 cursor 重置只影响一个公平窗口。

### 6.3 防饥饿老化

用户任务每完整等待 10 分钟提升一个 band：

```text
low -> normal -> high
normal -> high
high -> high
```

普通用户任务最高提升到 `high`，不能通过等待获得系统 `critical`。effective priority 只参与选择，不改写用户提交的原始 priority；重新入队保留最初 `queueEnteredAt`，因用户主动修改优先级而重新计算时也不清零等待年龄。

## 7. Agent 评测协作式抢占

v1 只允许**纯 Agent Evaluation Workflow**被抢占。包含 Deployment、CloudJob、数据库操作、回调或混合步骤的 Workflow 永远不可抢占，即使请求中设置 `preemptible=true`。

触发条件：

- 全局或目标项目已无可用 slot。
- 存在等待中的更高 effective priority 任务。
- 候选评测任务优先级更低、`preemptible=true`、尚未超过 3 次成功抢占。
- 同一候选距离上次抢占至少 5 分钟。
- 若目标项目配额已耗尽，候选必须来自同一项目；只有目标项目仍有可用配额、瓶颈仅为全局 slot 时，才允许跨项目选择候选。

协议：

1. Scheduler 以 CAS 将候选从 `running` 改为 `preempting`，但不立即释放 slot或终止 Runner。
2. Eval Runner 通过 [评测控制协议](agent-evaluation-job-design.md#10-checkpoint恢复与抢占) 收到 checkpoint 请求；在 30 秒端到端窗口内，最多使用前 20 秒排空在途请求，并为上传、事件上报和服务端校验保留后 10 秒。
3. Scheduler 验证 checkpoint 与 `taskId`、spec hash、dataset hash 和已完成 case cursor 一致后，通知 Runner 协作退出。
4. Runner 退出成功后，Scheduler 释放 slot，将任务恢复为 `waiting`，增加 `preemptionCount`；后续从 checkpoint 进入新 generation。
5. 30 秒内没有有效 checkpoint、Runner 拒绝或上传失败时，将状态恢复为 `running`，不删除 Kubernetes Job、不释放 slot，原评测继续执行。

抢占不会消耗业务失败重试次数。抢占任务的少量在途模型请求可能重复；Runner 必须使用 case execution id 和 checkpoint 去重，并在目标支持时传递幂等键。

## 8. API 与权限契约

### 8.1 全局任务 API

新增：

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | 按项目、类型、状态、优先级分页查询 Workflow Run |
| `GET` | `/api/v1/tasks/:taskId` | 查询统一任务详情和调度状态 |
| `PATCH` | `/api/v1/tasks/:taskId/scheduling` | 修改仍处于 `waiting` 的 priority、deadline、preemptible |
| `POST` | `/api/v1/tasks/:taskId/cancel` | 统一取消任务 |
| `GET` | `/api/v1/scheduler/summary` | 查询全局/项目 slot、排队深度、最老等待时间和 Leader 状态 |

列表查询使用 cursor pagination，默认 50、最大 200；`projectId` 是强制授权维度。普通调用方只能读写有权限的项目，平台管理员可以跨项目查询。响应沿用 Eruun 统一成功/错误 envelope，不返回 `runToken`、Secret 或内部凭据。

任务详情的调度投影至少包含：

```json
{
  "taskId": "task-123",
  "projectId": "project-a",
  "type": "workflow",
  "status": "waiting",
  "priority": "normal",
  "effectivePriority": "high",
  "queueEnteredAt": "2026-08-27T10:00:00Z",
  "deadlineAt": null,
  "workerId": null,
  "runGeneration": 2,
  "dispatchAttempts": 0,
  "preemptible": false,
  "preemptionCount": 0,
  "schedulingReason": "project_quota_exhausted"
}
```

现有 application-scoped Workflow 创建、查询、执行、审批和取消 API 保持兼容；它们写入或读取同一个 `WorkflowQueue`，取消逻辑委托给统一任务服务。旧客户端不需要迁移才能继续提交任务。

`preemptible=true` 只对 `agent_evaluation` 有效；对其他任务类型的创建或 PATCH 请求必须返回校验错误，不能保存一个实际不会生效的抢占标记。`critical` priority 和跨项目查询继续只允许平台管理员。

### 8.2 调度策略 SystemSetting

新增 `schedulerPolicy` 类型：

```json
{
  "mode": "legacy",
  "globalConcurrency": 100,
  "defaultProjectConcurrency": 10,
  "defaultProjectWeight": 1,
  "projectOverrides": {
    "project-a": {"maxConcurrency": 20, "weight": 2}
  },
  "agePromotionSeconds": 600,
  "queueTimeoutSeconds": 86400,
  "maxDispatchAttempts": 5,
  "preemption": {
    "enabled": true,
    "checkpointWaitSeconds": 30,
    "maxPreemptionsPerTask": 3,
    "cooldownSeconds": 300
  }
}
```

`mode` 取值：

- `legacy`：保留当前 Dispatcher 行为，用于升级兼容和回滚。
- `shadow`：计算并记录选择结果，但不改变任务、不派发消息。
- `enforced`：由新 Scheduler 唯一负责 `waiting -> queued`。

更新策略需要管理员权限、完整校验和审计记录。非法值或依赖不可用时 fail-fast，不静默回落到无限并发。

## 9. 容量调度边界

本文只回答“哪个 Workflow Run 现在获得执行 slot”。[资源补偿型 Scheduler](workflow-resource-capacity-scheduler.md) 回答“获得 slot 后，部署所需的 CPU/内存/GPU 是否满足，是否需要扩容节点”。

未来接入顺序：

```text
Global Scheduler selects Workflow Run
  -> Worker starts WorkflowCtl
  -> optional capacity admission plugin evaluates workload plan
  -> Workflow Steps/Jobs execute
```

v1 全局调度不实现 Node 容量估算、自动 ECS 扩容、Pod 绑定或 placement hint，避免把队列公平性与 Kubernetes 容量规划耦合。

## 10. 可观测性与运维

最低指标：

- `eruun_scheduler_cycle_seconds`
- `eruun_scheduler_waiting_tasks{priority}`
- `eruun_scheduler_active_slots{state}`
- `eruun_scheduler_project_quota_utilization`
- `eruun_scheduler_queue_wait_seconds{priority}`
- `eruun_scheduler_dispatch_attempts_total{result}`
- `eruun_scheduler_preemptions_total{result}`
- `eruun_scheduler_lease_recoveries_total{reason}`
- `eruun_scheduler_shadow_mismatch_total{reason}`

`projectId` 只在受控、数量有限的租户指标视图中使用，默认不作为 Prometheus label；完整项目与 task 关联进入结构化日志和 trace。告警至少覆盖：最老高优先级任务等待超过 5 分钟、Scheduler 无 Leader 超过 30 秒、派发失败率、租约恢复激增、slot 长期满载和 shadow mismatch。

## 11. 迁移、测试与验收

迁移顺序：

1. Additive schema 与索引上线，旧任务回填 `normal`、`_legacy` 和 `queueEnteredAt=createdAt`。
2. API 开始保存 scheduling spec，但 `schedulerPolicy.mode=legacy`。
3. 启用 `shadow`，在生产代表性流量下对比 legacy 与新算法至少一个完整高峰周期。
4. Worker 全部支持 v2 generation/token 后切到 `enforced`。
5. 清空旧消息后停止 legacy 生产，仍保留配置回滚入口一个发布周期。

测试必须覆盖：

- priority band、同项目 FIFO、WDRR 权重和项目配额。
- 10 分钟老化、防止提升到 `critical`、历史空 project 的隔离。
- 全局 100 slot、项目默认 10 slot，queued/running/preempting 的计数一致性。
- 并发 Scheduler CAS、Leader 切换、重复派发、过期 generation 和 token fencing。
- 24 小时 queue timeout、显式 deadline、5 次派发耗尽。
- 评测 checkpoint 成功抢占、30 秒无 checkpoint 回滚、冷却时间和最多 3 次限制。
- legacy/shadow/enforced 切换、旧 API 兼容和策略回滚。
- 100 并发 Workflow、每天 10,000 提交的负载测试，持续观察 DB 查询、锁等待与调度延迟。

验收标准是：任何时刻都不超出全局/项目配额；高优先级可快速获得 slot；同优先级项目长期份额符合权重且不会饥饿；故障恢复不丢任务、不接受旧 Worker 写入；只有具备有效 checkpoint 的 Agent 评测能够被抢占。
