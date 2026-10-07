# Eruun 分布式运行时设计

> 状态：Implemented Reference。本文描述当前单 Kubernetes 集群运行时契约；部署参数以 [Helm 部署契约](helm-deployment.md) 为准，工作流细节以 [Workflow 架构指南](workflow-architecture-guide.md) 为准。

## 1. 目标与边界

所有 Eruun 节点使用同一二进制、配置和 Deployment。节点竞争一个 Kubernetes Lease，当前 Leader 承接 HTTP/gRPC API、调度和 Controller 后台维护；其他节点执行 Worker 任务并继续参选。API、调度、观察和执行仍是内部职责，不再是四种部署角色。

这减少了独立角色的部署、配置和接管组合，也把 API、调度与后台维护的资源竞争和故障集中到一个 Leader。增加节点主要增加 Worker 容量和接班候选，不会增加活跃 API 或调度 Leader 数。是否需要进一步拆分，应根据 API 尾延迟、队列积压、控制循环耗时和恢复时间测量。

MySQL 中的 Workflow ownership 仍是执行事实源；角色切换不替代 generation/token fencing、任务续租、结果 outbox、DB 时钟或 Kubernetes UID 校验。当前边界不包含跨集群调度、跨地域多活、exactly-once 外部副作用或默认生产级数据面 HA。空间网络隔离仍依赖 CNI，见[账号与空间](account-auth-workspaces.md)。

## 2. 节点职责与业务入口

| 当前职责 | 工作 | 执行任务 |
| --- | --- | --- |
| Leader | 业务 HTTP/gRPC、鉴权与任务持久化；waiting/Cron 派发、lease reaper；状态投影、延迟任务、结果 outbox、制品交付与清理 | 停止领取新任务；升主前已认领的 Worker 任务继续完成并续租 |
| Worker＋候选 | 消费 dispatch，以数据库 ownership 认领和执行 Workflow/Job，等待资源就绪；参与同一个选举 | 并行执行，新任务受现有准入与并发限制 |

Chart 默认 `runtime.replicas=4`，稳定状态为 1 个 Leader＋3 个 Worker。单节点能当选并提供 API，但没有领取新任务的 Worker 容量；生产至少需要 2 个节点，默认建议 4 个，副本数不要求奇数。所有节点必须具备接任 Leader 所需的配置、依赖和权限。

集群通过固定 Service 访问业务 API。Service 保留 selector，其中 `eruun.io/runtime-id` 初始为 `unassigned`；Leader 为自身 Pod 设置值为 Pod UID 的同名标签，并通过 Service resourceVersion CAS 把 selector 指向自己。失主时只条件撤销自己的选择，不能清除新 Leader 的入口。EndpointSlice 与 Pod readiness 由 Kubernetes 原生控制器维护。健康 Worker 仍为 PodReady，但不会因此被该 Service 选中。

`--leader-service-name` / `ERUUN_LEADER_SERVICE_NAME` 指定集群入口 Service；集群部署设置该值，本地可留空并直接访问已当选节点。已接受业务请求的 HTTP 连接和全部 gRPC 连接绑定当前 Leader 任期，失主时关闭，包括空闲连接；Service 尚未收敛时，Worker 对业务 HTTP 返回 503 并关闭连接，对新 gRPC 连接直接断开，健康探针仍可用。客户端重新连接固定 Service 后先查任务状态，只有契约支持时才用相同幂等键重试，不能盲目重放创建任务或刷新会话。

Redis 仍用于缓存、应用变更锁和取消信号；选择 Kafka 只替换消息后端。提交应用 Workflow 的 Redis 锁续期失败会取消业务 context。同一应用的手动执行、Cron、数据库重置、版本自动执行和直接版本提交仍使用应用行锁与 READ COMMITTED 事务。版本提交验证预检快照，配置变化返回 HTTP 409（10043），避免旧快照覆盖；这些 D1 保障不会因只有一个 Leader 而删除。

## 3. 单 Leader 任期与切换

所有节点在同一 namespace 竞争 `--leader-lock-name` / `ERUUN_LEADER_LOCK_NAME` 指定的 Lease，默认 `eruun-runtime`。选举 identity 默认使用每次进程启动生成的 UUID；集群通过 `--pod-name` / `ERUUN_POD_NAME` 传入 PodName，仅用于查询和标记本 Pod，不作为 Lease holder。不要把 PodName 或固定节点名复用为 `ERUUN_ID`；显式覆盖 `--id` / `ERUUN_ID` 时必须保证每次进程实例唯一。不同安装应使用不同 Lease 名称和入口 Service，不能意外共享一个选举。

- 升主时停止 Worker 接单，使用独立的任期 context 启动 API、调度和 Controller 循环。已有任务沿原执行 context、generation/token/worker identity 继续，不为了升主等待全部长任务结束，也不重新授予完整超时。
- 失主时取消业务 API 和控制职责、条件撤销自己的 Service 选择，随后重入 Worker 并继续参选。健康 Worker 不因未当选而 readiness 失败。
- 进程退出与失主任期不同：退出还要限时排空已有 Worker 任务，超时后取消本地执行并依靠数据库租约恢复。

Kubernetes Lease 与 Service selector 更新不是跨系统事务，也不对全部外部副作用提供 fencing。网络分区、进程暂停和控制面拥塞仍需故障验证；不能承诺固定恢复秒数、连接无中断或绝无重叠执行。

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

非终态的底层持久化 helper 同样要求完整身份，空 token、worker 或零代次不能绕过事务。API 在首次 claim 前产生的终态回调可以有空执行身份，但仍必须锁定完整父状态快照。ownership 不匹配时，旧执行立即停止后续状态写入。外部系统仍需使用执行身份作为幂等键或提供补偿，因为 at-least-once 不能保证外部副作用 exactly once。

### 4.3 故障恢复

Scheduler lease reaper 周期扫描 lease 已过期且身份完整的 `queued/running` task，以 CAS 清理旧 ownership 并恢复为 `waiting`。下一次派发创建新的 generation/token。

Waiting task 到期判断也以数据库时间为准；时钟查询失败或返回零值时不派发。Scheduler 的 waiting task 与 cron schedule 查询都在数据库侧按到期时间过滤，并按 100 条固定批次处理。`workflow_queue(status, execute_at)` 与 `workflow_schedule(enabled, next_run)` 复合索引支撑这两条热路径；持续积压会由后续轮询继续排空，而不会把全量未到期记录加载到单个 Leader 进程。

Cron schedule 按 `next_run, id` 稳定排序，在同一进程的后续轮询中推进页码，读到末页后回到第一页。即使整批计划因错误、应用锁争用或无可运行日期而未推进 `next_run`，后面的到期计划也会获得处理机会。成功派发、删除或禁用计划导致候选集缩小时，移入前页的记录会在下一轮扫描中重新被读取；进程重启从第一页开始。分页不改写计划的 `next_run`、`last_run` 或幂等键，派发失败仍按原有事务语义回滚并返回错误。

进程启动不会全表重置 active task。未认领消息依赖 Redis AutoClaim 或 Kafka Rebalance；已经 ACK 的任务依赖数据库 lease reaper。

## 5. Job 执行身份

`JobInfo`、延迟载荷、结果载荷和 result outbox 携带同一 generation-aware 执行身份。Kubernetes Job 同时写入执行身份 annotation。

一次性延迟 Job 在发送队列通知前，先把完整载荷、到期时间和 `pending` 检查点写入 `JobInfo`。新队列通知使用 `version: 2`，只带 `executeAt`、`taskId`、`jobType`、`serviceName`、`executionKey`、`runGeneration` 和 `runToken`；Controller 读取数据库中的完整载荷，核对通知身份后执行。升级期间仍能读取未标版本、内嵌完整 Job 的旧通知，并继续比对完整载荷。Redis Stream 只负责降低到期发现延迟；Controller Leader 还会按 `(status, delay_state, delay_execute_at)` 索引轮询已到期检查点并直接恢复。因此 consumer group 被重建、Stream 被裁剪、Redis 暂时不可用或进程在写库后/入队前退出，都不会让已提交的延迟执行永久丢失。成功创建同身份 Kubernetes Job 并持久化 result outbox 后，检查点才变为 `dispatched`。

数据库恢复每次轮询最多读取 100 条记录，按记录 ID 倒序推进游标，到末尾后重新扫描。持续重试、无效载荷或扫描期间记录状态变化不会阻塞后续到期任务；新到达的记录在下一轮扫描中纳入。队列通知按执行键去重，被去重的消息释放处理标记并保持未确认状态，允许 Kafka 再次认领并在执行完成后确认。

Harbor 恢复资格、恢复任务剩余预算、Job 恢复准入预检查及持久化 Job retry 的 deadline/RetryAt 使用数据库时间。恢复时把剩余时长换算为本进程的单调计时预算，保持原 deadline，不因节点时钟差或接管而重新获得完整超时。数据库时钟缺失、失败或零值会停止推进；这不替代 Kubernetes、Runner 与数据库之间的实际时钟同步要求。

结果处理只消费与当前 `JobInfo` 和 Kubernetes Job annotation 匹配的结果；旧 generation 的迟到结果不能覆盖当前执行。确定性资源名用于重试复用，执行身份用于区分不同 generation。

结果通知也以数据库 outbox 为恢复来源。`result_dispatching_queue` 和 `result_queued` 有 60 秒补投宽限；消费者认领时写入独立 token 和 30 秒数据库租约，每 10 秒续期。到期回收必须同时匹配 state、token/消息 ID 和原租约，活跃消费者续期后旧扫描快照不能将其重新投递。重复通知不授予处理权限；旧 claim 不能再提交结果或删除 outbox。

结果写入在检查 claim 的事务内通过状态、执行代次与 attempt 的 CAS 保存终态与日志；并发取消等已提交终态不会被覆盖，仅最终持久化为 Completed 才按已记录的 Job UID 清理 Kubernetes 对象。保存失败保留现场；清理失败保留 outbox 供重试；重放已完成记录只补清理，不重新等待已删除 Job。按名称读取日志后还需校验 Pod UID 与 Job owner；读取失败、身份变化或无法确认身份均保留现场等待重试。结果 ACK 在清理与 outbox 收敛后发生。数据库与 Kubernetes 之间仍没有跨系统原子事务。

无需读取日志的例外是：Failed Pod 中的容器明确处于 Waiting、没有启动或重启记录，并经重新读取 Pod 确认相同 UID、owner 和状态；结果中记录该容器从未启动及 Pod 失败原因，避免旧失败尝试阻塞已成功 Job。缺少容器状态不视为从未启动。

升级需通过现有 schema migration 增加 outbox 的 `lease_expires_at`、`job_uid` 列。旧空租约记录先登记宽限而不是立即接管；旧 processing 记录的宽限包含其完整 Job timeout、30 秒删除、30 秒处理及 5 秒保存余量。旧终态记录没有已保存 UID 时不自动删除同名对象。历史结果协议的完整防护需旧进程排空并升级后成立。既有 Deadline/RetryAt 原值保留，不追溯校正历史节点偏差。具体回归与未验证的集群故障边界见[分布式设计审核的修复处置](distributed-design-audit-2026-10-06.md#8-pr-139-修复处置与验证边界)。

## 6. Informer 与 Worker

Controller Leader 的 Informer Manager 负责全局状态投影和应用状态同步。

每个 Worker 使用独立的 `ComponentReadyObserver`。当前 `KubernetesWorkloadObserver` 在 Worker 进程内维护共享 Pod informer cache，并按 application/component、期望镜像、annotation、Ready condition 和异常终态判断资源状态。这样 Worker 不依赖 Controller Leader 的进程内 waiter，也不需要每个 Job 各自执行 cluster-wide Pod List。

initial sync、List/Watch 重连和等待过程都受运行 context 控制；关闭或超时会返回明确错误，不把未知状态视为 Ready。

## 7. 启动与关闭

节点初始化 Kubernetes、MySQL schema、Redis/消息主题与 IoC 后，准备 Worker 观察和执行能力并参与选举。业务 API 与控制循环受当前任期约束，健康探针反映当前职责及依赖是否就绪；PodReady 不等于正在提供业务 API。

收到 SIGTERM 后先取消选举与任期入口，停止领取新任务，再以独立执行 context 排空已启动任务；上限由 `--workflow-worker-drain-timeout` 控制，默认 60 秒。超时后停止本地执行与续租，由后续 Leader 的 reaper 和 Worker 接管。关闭是有限预算，不承诺所有长任务都能在该窗口内自然完成。

## 8. 部署

Chart 使用一个 runtime Deployment 和 ServiceAccount，`runtime.replicas` 默认 4，`runtime.resources` 配置每个节点的相同资源。固定 Service 只选择当前 Leader，健康 Worker 的 readiness 不作为业务路由条件。资源、schema、RBAC 和 Quickstart 参数见 [Helm 部署契约](helm-deployment.md)。

项目仍处于开发阶段，直接使用当前配置部署统一节点。数据库仍按既有 schema 初始化与校验流程准备；启动后核验唯一 Lease holder、Service 选择和 Worker readiness。

任务 ownership、D1–D5 修复和历史 deadline/RetryAt 原值保持不变。新拓扑上线不是结果协议或恢复正确性的验收证明。

## 9. 依赖与安全

- MySQL 保存 Workflow、Job 和 ownership 状态；生产环境应使用经过验证的 HA 实例。
- Redis 用于缓存、应用变更分布式锁及可选的 Streams 消息；Kafka 可作为消息后端，Workflow 执行租约由 MySQL 管理。
- 内置 MySQL/Redis 只适合开发和演示。
- run token 属于执行凭据，不写入业务日志、trace 或指标标签。
- 所有节点都可能接任 Leader，因此使用统一运行身份与所需的显式 Kubernetes 权限；业务空间仍使用受限身份，任务 ownership 与授权边界不变。具体权限见 `helm-deployment.md`。

## 10. 验证边界

本拓扑的验证应覆盖统一节点装配、单 Lease 任期、健康 Worker、升主停止接单但保留旧任务、失主撤销 API/控制循环后恢复 Worker，以及 Service CAS 不误删新 Leader 选择。现有任务 lease CAS、heartbeat、reaper、结果 outbox、DB clock 和 UID 回归仍需保留。

模板检查应验证统一 Deployment/ServiceAccount、副本与资源字段、Service selector、RBAC、schema migration、PDB 与退出预算。真实集群还需验证 Leader/Worker 删除、长任务期间切主、HTTP/gRPC 重连、网络分区和依赖短暂不可用；单元或模板测试不能代替这些验收，也不能推导固定 RTO。
