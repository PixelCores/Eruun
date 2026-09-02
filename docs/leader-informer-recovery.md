# Leader、Informer 与执行租约恢复

> 状态：Current。本文说明四类运行角色、两类 Leader Election、Informer 生命周期以及 Workflow 数据库租约恢复边界。

## 角色与 Leader 契约

服务端通过 `--role` / `ERUUN_ROLE` 运行 `api`、`controller`、`scheduler`、`worker`，或用聚合角色 `all` 同时启用四类职责。角色不通过 Deployment 副本数推断，副本数也不要求奇数。

Controller 与 Scheduler 使用彼此独立的 Kubernetes Lease：

| 职责 | 默认 Lease | 配置 |
| --- | --- | --- |
| Controller | `eruun-controller` | `--controller-lock-name` / `ERUUN_CONTROLLER_LOCK_NAME` |
| Scheduler | `eruun-scheduler` | `--scheduler-lock-name` / `ERUUN_SCHEDULER_LOCK_NAME` |

`--duration` / `ERUUN_DURATION` 控制 election 的 `LeaseDuration`，默认 `15s`、最小 `4s`；`RenewDeadline` 按 LeaseDuration 约三分之二计算。`role=all` 不创建第三种历史 Lease，而是分别参与 Controller 与 Scheduler 两个 election；任一职责只在取得对应任期后启动。

每个 election 使用 `ReleaseOnCancel=false`；失去任期或进程关闭时，先同步停止该角色的 worker，再显式释放 Lease，避免新 Leader 与旧任期工作重叠。`--exit-on-lost-leader=true` 保留进程退出语义；设为 `false` 时，实例短暂退避并重新参选，作为 standby 保持运行和 ready。

## Controller 与 Informer

Controller Leader 任期包含：

- 创建新的 Informer runtime 并等待 initial sync。
- 启动 Controller event workers、状态同步和 adoption 协调。
- 任期结束时取消对应上下文并停止本轮 Informer。

再次成为 Controller Leader 时会重新创建 `SharedInformerFactory` 与 stop channel。Controller 的 Informer 负责状态投影；它不是 Workflow Worker 判断 Deployment、StatefulSet 或 Job Ready 的唯一来源。

Worker 注入独立的 `KubernetesWorkloadObserver`。每个 Worker 进程启动一条只观察带 `eruun.io/app-id` 标签 Pod 的共享 List/Watch，并等待 initial cache sync；各 Job 每 2 秒从本地 lister cache 检查 application/component、镜像、注解和 Ready 状态，不再逐 Job 发起 cluster-wide Pod List。Controller Leader 切换不会让 Worker 丢失 waiter，client-go 负责 watch 重连和过期 ResourceVersion 恢复，等待 context 取消仍会作为 Job 错误返回。

Worker observer 的 initial cache sync 最多等待 30 秒；持续 RBAC/List 错误或 API Server 不可达会返回启动错误并 fail-fast。

## Scheduler 与数据库租约

Controller Leader 负责延迟 Job、结果消息和结果 outbox 协调；Scheduler Leader 负责 waiting task dispatcher、定时任务派发和数据库 lease reaper；Worker 独立消费并执行任务。执行协议固定如下：

- Scheduler 以 CAS 为 `waiting` 任务增加 `runGeneration`、生成 `runToken`，写入 dispatch lease，并发送版本 2 消息。
- Worker 校验 generation/token 后，以 CAS 写入 `workerId` 并转为 `running`。
- Worker 默认每 10 秒续租；续租失败后重读权威任务。同一 owner 已写入终态时仅停止 heartbeat，让终态 callback 继续完成；generation/token/worker 不匹配时才取消本地执行。
- Scheduler 默认每 10 秒回收已过期的 `queued/running` 任务并恢复为 `waiting`。
- 旧 generation 的 WorkflowQueue 状态写入被 ownership 条件拒绝；JobInfo 使用 generation-scoped execution key。版本更新的 cleanup 跨 generation 复用同一检查点记录时，以 `runGeneration/executionKey/attempt` CAS 转移写入权，旧 generation 的延迟 SaveInfo 会被忽略，不能覆盖新一代状态或身份。
- WorkflowQueue 状态写入结果不确定或执行 ownership 丢失时，本地取消使用基础设施接管原因；旧执行不写入 `cancelled` JobInfo，仍由当前 Worker 保留租约并按权威任务快照恢复。用户取消继续写入明确的 `cancelled` 终态。
- 延迟 Job、结果消息和结果 outbox 透传 `executionKey/runGeneration`；结果写入按该身份精确查找 JobInfo，同名 Kubernetes Job 也用注解校验身份后才允许收集日志或删除。

在 30 秒 lease 和 10 秒 reaper 默认值下，系统目标是在故障后 60 秒内让可恢复任务重新进入派发。消息队列提供 at-least-once 交付，Redis task-run lock 用于降低重复启动概率，数据库 token 是最终 fencing 事实。

## 运行建议

- 所有角色都直接使用 generation/token ownership，不存在 v1 消息、空 ownership 或关闭 fencing 的运行组合。
- Controller、Scheduler 和 Worker 副本数都可按容量与可用性独立设置，不要求奇数。
- Worker 收到终止信号后停止领取新消息，默认最多排空 60 秒；Helm Chart 和 `deploy/eruun-stack.yaml` 都提供 90 秒 `terminationGracePeriodSeconds`，超时后取消执行并由数据库 lease reaper 接管。
- Workflow 数据库 lease 的写入、续租、释放和过期比较统一使用 MySQL 的微秒级 Unix 时间；运行节点的绝对时钟偏差和 DSN 时区不参与 ownership 转移。历史 Kubernetes Lease 迁移门禁继续使用本机经过时间观察。

完整字段和消息协议见 [企业级分布式运行时设计](enterprise-distributed-runtime-design.md)；当前不提供跨版本迁移或回滚协议。
