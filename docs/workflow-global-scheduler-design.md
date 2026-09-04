# Eruun Workflow 全局调度方向

> 状态：Draft / Proposal。本文描述跨 Workflow Run 的优先级、公平性、配额、容量准入和可抢占需求；当前 Scheduler 尚未实现这些策略，本文不定义路由、表字段、固定默认值或唯一算法。

## 1. 当前事实

当前 Scheduler Leader 周期处理 waiting Workflow，通过数据库 CAS 生成新的 generation/token 后发布 dispatch。Worker 消费消息、认领数据库 lease、执行 Workflow 并维护 heartbeat；Scheduler reaper 回收过期 ownership。

当前已经具备：

- 数据库作为任务状态和执行 ownership 事实源。
- Redis Streams 或 Kafka 的 at-least-once 派发。
- generation/token fencing、Worker heartbeat 和过期恢复。
- Workflow 内部的 StepByStep/DAG、Job priority bucket、审批和取消。
- 每个 Worker 进程内的并发限制。

当前没有跨 Workflow Run 的业务优先级、项目公平性、持久化全局配额、GPU 容量准入或协作式抢占。

## 2. 设计原则

- 调度单位优先保持为 Workflow Run，不把每个 Step、Job 或 Pod 复制到第二份顶层队列。
- 状态与 slot/配额预留必须以数据库事务或 CAS 为准，不能以消息队列或单个 Scheduler 内存为事实源。
- 策略影响“谁先执行”，不改变 Workflow 内部步骤、审批、失败清理和 callback 语义。
- 项目隔离、优先级和容量选择必须可解释，调度原因要进入查询、日志或 trace。
- 不在没有代表性负载数据时固定全局并发、老化间隔、重试次数或具体公平算法。

## 3. 目标能力

### 优先级与公平性

调用方可以表达有限的业务优先级，平台保留系统恢复优先级。相同优先级下，不同 workspace/project 应长期获得与其策略一致的执行份额，单个高流量项目不能永久阻塞其他项目。

老化、防饥饿和权重是候选机制，具体算法必须通过确定性模拟和负载测试选择。公共 API 不应暴露算法内部计数器。

### 配额与并发

平台需要可持久化地约束全局及项目活跃 Workflow 数量。哪些状态占用 slot、审批等待是否释放、Leader 切换后如何重建，都必须成为单一状态机的一部分。

配额配置的默认值和覆盖范围由部署规模决定；非法或无法执行的策略要明确失败，不能静默退回无限并发。

### 容量准入

队列调度回答“哪个 Workflow 获得执行机会”；容量准入回答“该 Workflow 请求的 CPU、内存、GPU、存储或特定设备能否运行”。两者可以串联，但不应耦合成一个不可测试的控制器。

首个容量准入只需要识别明确不可满足或需要等待的平台能力，不替代 Kubernetes Scheduler，也不直接决定 Pod 绑定节点。

### Deadline 与取消

任务可以有明确 deadline 或平台排队上限。超过 deadline 的任务进入可解释终态；基础设施恢复、用户取消和业务失败必须保持不同原因，不能互相覆盖。

## 4. Agent 评测与抢占

评测可能是长期、低优先级且可以 checkpoint 的任务，但“Agent evaluation”这个名称本身不能赋予可抢占性。

只有同时满足以下条件才允许设计协作式抢占：

- 任务没有不可补偿的外部副作用。
- Runner 能生成与输入、目标和执行版本绑定的可验证 checkpoint。
- 在途请求重复的影响已记录并可接受。
- 原 Worker 在 checkpoint 成功前继续持有 ownership 和容量。
- checkpoint 失败时能够恢复原执行，而不是丢失任务或释放仍在使用的资源。

具体状态、时限、次数和控制协议由评测实现与负载数据决定，不在本文预设。

## 5. 状态与所有权约束

任何实现都必须保持：

- Scheduler 的选择、容量预留和任务状态变更使用同一个权威数据库边界。
- dispatch 失败可重试，但不会绕过 generation/token 或重复占用配额。
- Worker 只有持有当前 ownership 才能写入进度和终态。
- Leader 切换后可从数据库重建调度视图；内存游标丢失最多影响短期公平性，不影响正确性。
- 旧任务的迁移和回滚模式需要显式定义，不能让两个 Scheduler 同时生产。

## 6. 可观测性

最低观测面包括：

- 各优先级的排队深度和等待时间。
- 全局及项目 slot/配额使用率。
- 任务选择、拒绝、等待和恢复原因。
- dispatch 重试、ownership 冲突、lease 恢复和 Leader 任期。
- 容量准入的资源类型与不可满足原因。
- 若实现抢占，其请求、成功、回退和重复工作量。

workspace/project 等高基数身份默认进入结构化日志或 trace，不直接作为无界 Prometheus label。

## 7. 实施顺序

1. 采集当前排队、执行时长和资源需求，不改变调度结果。
2. 增加可持久化的优先级和项目并发策略，以 shadow 模式对比当前选择。
3. 用确定性测试和代表性负载选择公平算法，再启用强制模式。
4. 把通用容量准入作为独立门禁接入。
5. 只有评测 checkpoint 闭环完成并出现真实资源争用后，才增加协作式抢占。

升级为 Current 需要 schema/API（如有）、并发 CAS、Leader 切换、消息重复、配额、老化、公平性、deadline、回滚和负载测试证据。
