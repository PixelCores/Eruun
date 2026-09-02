# Eruun 分布式运行时设计

> 状态：Implemented Reference。本文描述当前单 Kubernetes 集群运行时契约；部署参数以 [Helm 部署契约](helm-deployment.md) 为准，工作流细节以 [Workflow 架构指南](workflow-architecture-guide.md) 为准。

## 1. 目标与边界

当前运行时把 API、Controller、Scheduler 和 Worker 的职责显式分离，并以数据库记录作为 Workflow 执行事实源。目标是：

- 各角色可以独立扩缩。
- Controller 与 Scheduler 使用不同 Kubernetes Lease，互不扩大故障域。
- Scheduler 派发和 Worker 执行始终携带 generation/token ownership。
- Worker 或 Leader 退出后，过期任务由数据库 lease reaper 恢复。
- Worker 不依赖 Controller Leader 的进程内 informer 才能等待资源就绪。
- 关闭时先停止 HTTP intake 和 Worker intake，再在有限时间内排空已启动任务。

当前边界不包含跨 Kubernetes 集群调度、跨地域多活、exactly-once 外部副作用、默认生产级 MySQL/Redis HA、NetworkPolicy 或指标驱动 HPA。

## 2. 角色模型

同一二进制通过 `--role` / `ERUUN_ROLE` 选择一个运行角色：

| 角色 | 职责 | Leader Election |
| --- | --- | --- |
| `api` | HTTP API、鉴权、任务持久化、查询和取消入口 | 否 |
| `controller` | Informer 状态投影、延迟任务、结果队列和 outbox 协调 | Controller Lease |
| `scheduler` | waiting task 派发和过期执行租约回收 | Scheduler Lease |
| `worker` | 消费 dispatch、执行 Workflow 与 Job | 否 |

当前部署固定渲染四类独立角色；不再提供组合进程 `all` 运行形态。各角色副本数独立配置，且不存在奇数副本约束。

API 角色注册完整业务 API；非 API 角色只注册健康路由。Follower Controller/Scheduler 保持进程可用并等待下一任期，readiness 不把“当前不是 Leader”当作失败。

## 3. 双 Leader 契约

Controller 和 Scheduler 分别使用：

```text
<controller-lock-name>
<scheduler-lock-name>
```

两个名称必须不同。每次获得任期都会创建独立 role context；失去任期时只停止该角色本次任期内的 goroutine 和写入，然后继续等待重新参选。Worker 不绑定 Leader 任期。

## 4. Workflow ownership

### 4.1 派发

Scheduler 对 `waiting` task 执行 CAS，生成新的：

- `runGeneration`
- `runToken`
- dispatch 时间与 lease deadline

随后发布版本 2 dispatch。Worker 只接受携带完整 `taskId/runGeneration/runToken` 的消息，并在 ownership CAS 成功后把任务置为 `running`、写入 `workerId` 和新的 lease deadline。

dispatch、Worker claim、heartbeat、显式释放和 Scheduler reaper 都以 MySQL 的微秒级 Unix 时间为权威时间。运行节点的墙钟和 DSN 时区不参与数据库 lease 的到期判断，因此节点时钟偏差不会让 Scheduler 提前接管仍在续租的 Worker。

消息队列是 at-least-once 分发通道，数据库中的 `WorkflowQueue` 才是执行状态和 ownership 的事实源。缺少版本或完整 ownership 的 dispatch 不进入执行路径。

### 4.2 心跳与状态写入

Worker 在执行期间周期性续租。续租和任务状态更新都必须匹配：

```text
taskId + runGeneration + runToken + workerId
```

ownership 不匹配时，旧执行立即停止后续状态写入。外部系统仍需使用执行身份作为幂等键或提供补偿，因为 at-least-once 不能保证外部副作用 exactly once。

### 4.3 故障恢复

Scheduler lease reaper 周期扫描 lease 已过期且身份完整的 `queued/running` task，以 CAS 清理旧 ownership 并恢复为 `waiting`。下一次派发创建新的 generation/token。

Scheduler 的 waiting task 与 cron schedule 查询都在数据库侧按到期时间过滤，并按 100 条固定批次处理。`workflow_queue(status, execute_at)` 与 `workflow_schedule(enabled, next_run)` 复合索引支撑这两条热路径；持续积压会由后续轮询继续排空，而不会把全量未到期记录加载到单个 Leader 进程。

Cron schedule 按 `next_run, id` 稳定排序，在同一进程的后续轮询中推进页码，读到末页后回到第一页。即使整批计划因错误、应用锁争用或无可运行日期而未推进 `next_run`，后面的到期计划也会获得处理机会。成功派发、删除或禁用计划导致候选集缩小时，移入前页的记录会在下一轮扫描中重新被读取；进程重启从第一页开始。分页不改写计划的 `next_run`、`last_run` 或幂等键，派发失败仍按原有事务语义回滚并返回错误。

进程启动不会全表重置 active task。未认领消息依赖 Redis AutoClaim 或 Kafka Rebalance；已经 ACK 的任务依赖数据库 lease reaper。

## 5. Job 执行身份

`JobInfo`、延迟载荷、结果载荷和 result outbox 携带同一 generation-aware 执行身份。Kubernetes Job 同时写入执行身份 annotation。

一次性延迟 Job 在发送队列通知前，先把完整载荷、到期时间和 `pending` 检查点写入 `JobInfo`。Redis Stream 只负责降低到期发现延迟；Controller Leader 还会按 `(status, delay_state, delay_execute_at)` 索引轮询已到期检查点并直接恢复。因此 consumer group 被重建、Stream 被裁剪、Redis 暂时不可用或进程在写库后/入队前退出，都不会让已提交的延迟执行永久丢失。成功创建同身份 Kubernetes Job 并持久化 result outbox 后，检查点才变为 `dispatched`。

数据库恢复每次轮询最多读取 100 条记录，按记录 ID 倒序推进游标，到末尾后重新扫描。持续重试、无效载荷或扫描期间记录状态变化不会阻塞后续到期任务；新到达的记录在下一轮扫描中纳入。队列通知按执行键去重，被去重的消息释放处理标记并保持未确认状态，允许 Kafka 再次认领并在执行完成后确认。

结果处理只消费与当前 `JobInfo` 和 Kubernetes Job annotation 匹配的结果；旧 generation 的迟到结果不能覆盖当前执行。确定性资源名用于重试复用，执行身份用于区分不同 generation。

## 6. Informer 与 Worker

Controller Leader 的 Informer Manager 负责全局状态投影和应用状态同步。

每个 Worker 使用独立的 `ComponentReadyObserver`。当前 `KubernetesWorkloadObserver` 在 Worker 进程内维护共享 Pod informer cache，并按 application/component、期望镜像、annotation、Ready condition 和异常终态判断资源状态。这样 Worker 不依赖 Controller Leader 的进程内 waiter，也不需要每个 Job 各自执行 cluster-wide Pod List。

initial sync、List/Watch 重连和等待过程都受运行 context 控制；关闭或超时会返回明确错误，不把未知状态视为 Ready。

## 7. 启动与关闭

启动顺序为：

1. 使用父 context 初始化 Kubernetes、MySQL schema、默认系统设置、Redis/Kafka 和 IoC。
2. 按角色注册业务或健康路由。
3. 启动角色选举、Worker observer 和 Worker intake。
4. 启动 HTTP server。

收到 SIGTERM 后：

1. 父 context 立即停止 HTTP intake。
2. Worker 停止领取新消息。
3. 已启动 Workflow 使用独立 execution context 排空，最长由 `workflow-worker-drain-timeout` 控制。
4. 超时后取消剩余执行，使租约停止续期并由 Scheduler reaper 接管。
5. 停止 Controller/Scheduler 任期并结束进程。

HTTP shutdown 与 Worker drain 并行执行；外层进程等待也有明确超时，不会因启动迁移或运行 goroutine 永久阻塞退出。

## 8. Helm 拓扑

Chart 固定渲染 API、Controller、Scheduler、Worker 四个独立 Deployment 和 ServiceAccount；Service 只选择 API Pod。每个角色通过 `runtime.roles.<role>` 独立配置副本数和 resources，PDB 与 topology spread 按角色生成。旧的 `runtime.mode`、`runtime.split`、顶层 `replicaCount` 和 `resources` 会被 schema 拒绝。

角色资源名通过统一 helper 为 `-api/-controller/-scheduler/-worker` 预留长度，即使 fullname 达到 63 字符也保持唯一且符合 Kubernetes 名称限制。

Workflow fencing 固定启用，Chart 不暴露关闭开关、v1 兼容开关、cutover acknowledgement 或运行时迁移门禁。values 只描述目标拓扑。

## 9. 依赖与安全

- MySQL 保存 Workflow、Job 和 ownership 状态；生产环境应使用经过验证的 HA 实例。
- Redis 用于缓存、应用变更分布式锁及可选的 Streams 消息；Kafka 可作为消息后端，Workflow 执行租约由 MySQL 管理。
- 内置 MySQL/Redis 只适合开发和演示。
- run token 属于执行凭据，不写入业务日志、trace 或指标标签。
- Scheduler 只需要 namespace-scoped Leader Election 权限；API、Controller 和 Worker 当前共享资源管理 ClusterRole，进一步细分 RBAC 属于后续安全工作。

## 10. 验证要求

- 单元测试覆盖角色装配、双 Leader、lease CAS、heartbeat、reaper、消息身份校验和关闭时序。
- race 测试覆盖 Worker 生命周期、Leader 回调、observer 和 Workflow 状态写入。
- Helm 模板测试覆盖固定四角色对象数量、Service selector、PDB、termination grace、ServiceAccount 引用和 63 字符 fullname 下的角色名唯一性。
- 集群验收覆盖随机删除 Worker、Controller Leader、Scheduler Leader，以及 Redis、Kafka、MySQL 短暂不可用后的恢复。
