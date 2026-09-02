# 工作流任务去重方案

> 状态：Implemented Reference。本文记录当前 workflow task ownership、去重与幂等契约。

目标：允许 at-least-once 消息投递，同时保证同一时刻只有当前 generation/token/worker 能推进 workflow task 和对应 Job 结果。

## 固定协议

1. **Scheduler 创建执行代次**
   - Scheduler Leader 用数据库 CAS 将 `waiting` 更新为 `queued`。
   - 同一次 CAS 增加 `run_generation`、生成不可猜测的 `run_token`，并写入分发租约。
   - 只发布包含完整 generation/token 的版本 2 dispatch；不存在旧消息兼容分支。

2. **Worker 认领 ownership**
   - Worker 只接受版本 2 dispatch，并以 `taskId + runGeneration + runToken` 对 `queued -> running` 执行 CAS。
   - 认领成功后写入 `worker_id/heartbeat_at/lease_expires_at`；重复或旧消息无法再次认领。
   - 工作流进度、终态、审批检查点、Job 结果和心跳写入都必须携带完整 ownership 条件，缺失字段时 fail-closed。

3. **租约恢复**
   - Worker 周期性续租；停止 intake 后只在 drain timeout 内继续已认领执行。
   - Scheduler Leader 的 lease reaper 只回收租约过期且 generation/token/lease timestamp 仍完全匹配的 `queued/running` 任务。
   - 回收会清空旧 token/worker 并恢复为 `waiting`；下次分发创建更高 generation，旧 Worker 因 fencing 不能覆盖新执行。

4. **Job 与消息幂等**
   - Delay 和 Result 内部消息都要求 `executionKey + runGeneration`，缺失时拒绝。
   - Job 与 Result Outbox 使用执行身份生成确定性键；结果更新同时过滤 execution key 和 generation。
   - Redis AutoClaim 或 Kafka Rebalance 可以重复交付消息，但不能绕过数据库 ownership。

## 运行约束

- 生产 datastore 必须实现多条件 CAS；不支持时服务或写入路径直接报错。
- Redis/Kafka 是消息后端选择，不改变上述 ownership 协议。
- Workflow 执行认领、续租和状态写入统一由数据库 generation/token/worker ownership 约束；Worker 不再获取 Redis task-run lock。
