# Leader、Informer 与执行租约恢复

> 状态：Current。本文说明同构节点的单 Leader 任期、Informer 生命周期以及 Workflow 数据库租约恢复边界。

## 节点与 Leader 契约

所有节点使用相同配置，竞争一个 Kubernetes Lease。`--leader-lock-name` / `ERUUN_LEADER_LOCK_NAME` 默认 `eruun-runtime`；Lease identity 默认是每进程 UUID；集群使用 `--pod-name` / `ERUUN_POD_NAME` 提供本 Pod 名，用于入口维护。不要把 PodName 复用为 Lease identity。Leader 承接业务 HTTP/gRPC、Scheduler 和 Controller 后台维护，其他节点执行 Worker 任务并持续参选。不存在静态角色参数或单独的 Controller/Scheduler 选举。

`--duration` / `ERUUN_DURATION` 控制 election 的 `LeaseDuration`，默认 `15s`、最小 `4s`；这些是选举参数，不是完整业务恢复时限。集群入口由 `--leader-service-name` / `ERUUN_LEADER_SERVICE_NAME` 指定；Leader 通过 Service resourceVersion CAS 选择带自身 Pod UID 标签的 Pod，Kubernetes 维护 EndpointSlice。

升主只停止新任务接单，已有 Worker 执行继续使用原 context、ownership 和心跳。失主取消 API、调度和 Controller 任期，条件撤销自己的入口选择，再恢复 Worker 接单。无需因为升主先等待所有长任务完成；也不能因为成为 Leader 就取消原任务或刷新其 deadline。

健康 Worker 保持 PodReady，但不会被业务 Service 选中。切主可能使 HTTP/gRPC 流和端口转发断开，客户端应重连、查状态并按幂等契约安全重试。Lease、入口切换和数据库不是一个事务，不能将正常关闭顺序解释为绝无脑裂或外部副作用 exactly-once。

## Controller 与 Informer

Leader 任期内包含：

- 创建新的 Informer runtime 并等待 initial sync。
- 队列准备成功后，在同一 Leader 运行实例中启动 Workflow 调度与协调、状态同步和 adoption 协调。
- 任期结束时取消对应上下文并停止本轮 Informer。

再次成为 Leader 时会重新创建 `SharedInformerFactory` 与 stop channel。Controller 的 Informer 负责状态投影；它不是 Workflow Worker 判断 Deployment、StatefulSet 或 Job Ready 的唯一来源。

Worker 注入独立的 `KubernetesWorkloadObserver`。每个 Worker 进程启动一条只观察带 `eruun.io/app-id` 标签 Pod 的共享 List/Watch，并等待 initial cache sync；各 Job 每 2 秒从本地 lister cache 检查 application/component、镜像、注解和 Ready 状态，不再逐 Job 发起 cluster-wide Pod List。Leader 切换不会让 Worker 丢失 waiter，client-go 负责 watch 重连和过期 ResourceVersion 恢复，等待 context 取消仍会作为 Job 错误返回。

Worker observer 的 initial cache sync 最多等待 30 秒；持续 RBAC/List 错误或 API Server 不可达会返回启动错误并 fail-fast。

独立 command、全部 eval 和采用 retry checkpoint 的 Application Job 另共享带 `app.kubernetes.io/managed-by=eruun` 的 typed Job List/Watch。通知按 namespace/name 合并到等待器的一槽唤醒通道；正常运行不再每 2 秒查询 Kubernetes Job 或父任务 DB。终态候选、缓存缺失及身份异常经权威 GET 与 UID/执行身份核验，提交结果仍须 DB fencing。存量无标签 Job 仅在确认原 UID 和 ownership 后补标，selector 退出不能当作删除证据。普通非 retry Application InstantJob 的原等待路径仍保留。

配置 Harbor 时，节点为 API/Controller 提供限定 `sandboxes.agents.kruise.io/v1alpha1` 的 dynamic informer 与 task-id Pod 缓存；统一节点均须具备接任能力，不能继续按旧静态角色估算缓存副本数。初始未同步返回不可用；缓存裁掉 managedFields，保留资源身份和状态。基础、派生 typed/dynamic/空间 client 共享本进程 RateLimiter；不同进程预算仍相加，Watch 事件处理与 DB 协调不按普通 HTTP QPS 计数。

共享观察没有消除所有权威读取。`authorizeRunner` 每次有效事件仍 GET Runner Pod 和所属 Job，随后进行数据库身份与状态核验。按 10000 个 Runner、名义 15 秒一次 heartbeat 估算，仅这两次 GET 的请求需求约为 `10000 / 15 × 2 ≈ 1333/s`，还未包含 phase/progress、Sandbox 申请、结果上传、重连和其他 API 请求；实际节奏受响应时间与重试影响。这是待测流量估算，不是容量结论。`--kube-api-qps` / `ERUUN_KUBE_API_QPS` 默认每进程 100，Burst 默认 300；要按当前 Leader、Worker 节点及 ACK 控制面预算测量限速等待，不能只提高 Worker slot，或把 Watch 事件数当作 HTTP QPS。当前保留权威鉴权，不以缓存替代撤销和 UID 核验。

## Scheduler 与数据库租约

Leader 内部的协调职责负责延迟 Job 和数据库结果 outbox；调度职责负责 waiting task dispatcher、定时任务派发和数据库 lease reaper。两类循环共享同一任期生命周期，Worker 独立消费并执行任务。执行协议固定如下：

- Scheduler 通过唯一服务层入口认领 `waiting` 任务；延迟任务先在应用锁内检查未完成的 StatefulSet 清理，再以 CAS 增加 `runGeneration`、生成 `runToken`，写入 dispatch lease，并发送仅含版本、task ID、generation 和 token 的版本 2 消息。
- Worker 校验 generation/token 后，以 CAS 写入 `workerId` 并转为 `running`。
- Worker 默认每 10 秒续租；续租失败后重读权威任务。同一 owner 已写入终态时仅停止 heartbeat，让终态 callback 继续完成；generation/token/worker 不匹配时才取消本地执行。
- Scheduler 默认每 10 秒回收已过期的 `queued/running` 任务并恢复为 `waiting`。
- 旧 generation 的 WorkflowQueue 状态写入被 ownership 条件拒绝；JobInfo 使用 generation-scoped execution key。版本更新的 cleanup 跨 generation 复用同一检查点记录时，以 `runGeneration/executionKey/attempt` CAS 转移写入权，旧 generation 的延迟 SaveInfo 会被忽略，不能覆盖新一代状态或身份。
- WorkflowQueue 状态写入结果不确定或执行 ownership 丢失时，本地取消使用基础设施接管原因；旧执行不写入 `cancelled` JobInfo，仍由当前 Worker 保留租约并按权威任务快照恢复。用户取消继续写入明确的 `cancelled` 终态。
- 延迟 Job 和结果 outbox 透传 `executionKey/runGeneration`；结果写入按该身份精确查找 JobInfo，同名 Kubernetes Job 也用注解校验身份后才允许收集日志或删除。

默认 lease 为 30 秒、reaper 周期 10 秒、每轮最多回收 100 条；新增 `--workflow-lease-reaper-batch-size` / `ERUUN_WORKFLOW_LEASE_REAPER_BATCH_SIZE`，范围 1..10000。若 10000 条同时过期，默认批次仅回收就约需 100 轮，不能承诺 60 秒完成接管。调整批次前测 MySQL 锁与连接压力，并分别计量回收、派发、准入、健康资源重新关联。准入轮失败会记录错误，但不再直接跳过 Workflow 派发；每个派发仍通过既有 DB ownership，执行新 Job 仍须自己的准入门禁。消息队列保留 at-least-once 与 generation/token/worker fencing。

Worker 每 30 秒记录 `workflow runtime stats`：controller/等待槽位/续租任务数量、累计槽位等待与续租耗时/失败、goroutine、Go heap 和 GC。计数反映 Workflow 管理开销，不代表 Kubernetes Job/trial 实际运行数；平均累计耗时不能代替 p99。固定资源下按 100/250/500/1000 controller 上限比较，见[手动容量阶梯](../examples/agent-evaluation/load-test/README.md#固定单-worker-的手动容量阶梯)。

## 运行建议

- 所有节点都直接使用 generation/token ownership，不存在 v1 消息、空 ownership 或关闭 fencing 的运行组合。
- 统一节点副本数通过 `runtime.replicas` 配置，默认 4；至少两个节点才有新任务 Worker 容量，不要求奇数。
- Worker 收到终止信号后停止领取新消息，默认最多排空 60 秒；Helm Chart 和 `deploy/eruun-stack.yaml` 都提供 90 秒 `terminationGracePeriodSeconds`，超时后取消执行并由数据库 lease reaper 接管。
- Workflow 数据库 lease 的写入、续租、释放和过期比较统一使用 MySQL 的微秒级 Unix 时间，由 `TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6))` 直接计算，避免数据库会话本地时间在夏令时重复小时内的转换歧义。运行节点的绝对时钟偏差和 DSN 时区不参与 ownership 转移。Kubernetes Leader Lease 与业务数据库 lease 是不同协议，不可混为同一个到期判断。

完整字段和消息协议见 [企业级分布式运行时设计](enterprise-distributed-runtime-design.md)，部署参数见 [Helm 部署契约](helm-deployment.md)。

## 数据库时钟回归验证

将 `MYSQL_TEST_DSN` 设置为测试 MySQL 实例的 DSN，并确保实例已加载 `America/New_York` 时区表，然后运行：

```bash
go test -tags=integration ./pkg/apiserver/infrastructure/datastore/sql -run TestCurrentDatabaseTimeIntegrationAcrossDaylightSaving -count=1 -v
```

测试在独立连接上设置会话时间与时区，覆盖 UTC、固定偏移及夏令时切换边界，并检查微秒精度；不修改表结构或业务数据。未设置 `MYSQL_TEST_DSN` 时跳过此测试。
