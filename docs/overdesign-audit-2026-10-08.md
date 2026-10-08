# 过度设计审计（2026-10-08）

> 状态：Historical / Audit。审查基线为 `main@fc37007d9bb10d355015c1dd2f9bef2d2513702f`（PR #141 合并后）。本文只记录发现与最小简化边界，不包含源码改动。下文描述的是基线时的状态；Draft / Proposal 不作为当前能力。

代码路径省略 `pkg/apiserver/` 前缀；以 `cmd/`、`deploy/`、`docs/` 开头的路径相对仓库根目录。行号固定在基线提交，可在 `https://github.com/PixelCores/Eruun/blob/fc37007d9bb10d355015c1dd2f9bef2d2513702f/pkg/apiserver/<path>#L<line>` 查看。

## 1. 结论与判断标准

本轮确认 **19 项**（O29–O47），并复核了 1 项历史待评估项（B1）。没有证据支持推翻 API、Domain、Workflow、Infrastructure 的分层，也没有证据支持删除租约、fencing、outbox、CAS、UID 校验或 PVC 删除保护。

剩余的过度设计集中在五种形态：

1. 只有一种生产实现的“可选能力”或接口族，调用点仍要断言并保留降级分支；
2. 只有一个合法值的选择器或恒定开关，例如 `datastore-type`、`cache-type`、`noCache`；
3. 同一事实维护多份表示，例如授权表、清理标记结构、步骤校验、超时上限；
4. 只靠测试保活的路径，例如 `WorkflowService` 的死方法、Pod coordinator 的 observe 机制；
5. 开发阶段仍保留的旧格式兼容和旧安装迁移代码。

其中 **4 项定为 P2**：冗余结构已经造成或掩盖了行为差异。

- O29：延迟任务派发前的 StatefulSet 清理 fence 只存在于无生产调用的路径。
- O30：写入与 Try 的步骤校验已经分歧。
- O31：Current 文档承诺了未实现的更新策略。
- O32：三处 batch Job 删除没有指定传播策略。

其余为 P3 维护债务。所有结论都是代码阅读、生产调用检索和定向测试的结果，**没有线上事故证据，也没有测得性能收益**。

**判断标准。** 过度设计指为当前不存在的需求付出的结构成本。“代码长”或“层次多”本身不构成证据。每一项都必须说明当前真实需求、额外维护点，以及删除后仍需保留的行为。

**编号。**

- O01–O21 见已合入 `main` 的历史审计；
- O22–O28 由草稿 PR #124 使用，对应修复已经通过 #125–#130 合入；
- 本文从 O29 开始编号，避免与上述编号冲突。

## 2. 汇总

| 编号 | 级别 | 问题 | 额外成本 | 整改边界与契约影响 |
| --- | --- | --- | --- | --- |
| O29 | P2 | `WorkflowService` 有 5 个方法无生产调用；延迟任务派发前的 StatefulSet 清理 fence 只存在于其中 | 测试替身要实现死方法；4 个 fence 测试只证明死路径 | 先决定是否把 fence 接入真实派发，再删除死方法；仅影响 Go 符号 |
| O30 | P2 | Workflow 步骤在写入和 Try 各有一套校验，mode、stepType、重名已分歧 | 规则要两处同步；Try 拒绝的请求写入能成功 | 抽取共享叶子规则；写入变严属于 HTTP 行为变更 |
| O31 | P2 | 版本更新 `strategy` 四值枚举不影响执行，Current 文档却承诺 canary / blue-green | DTO、proto、文档暗示不存在的能力；对应错误码从未使用 | 只接受空值或 `rolling`，修正文档；请求校验变严 |
| O32 | P2 | batch Job 删除有多处手写实现，3 处没有指定传播策略 | 前置条件、传播策略、UID 等待各写一份；可能遗留 Pod | 共享删除与等待逻辑，显式传入策略；保留 Orphan 例外 |
| O33 | P3 | DataStore 的 5 个“可选能力”接口只有一种生产实现 | 27 个文件 63 处断言；保留无事务或默认隔离级别的降级分支 | 并入必需接口，删除降级分支；仅影响 Go 符号 |
| O34 | P3 | Workflow 运行配置有无入口字段、矛盾默认值和超时双表示；附带一处单位缺陷 | 退出分支不可达；13 个访问器重复回退 | 删除字段与分支，统一使用 Duration；flags 不变 |
| O35 | P3 | `datastore-type` / `cache-type` 只有一个合法值；缓存 `noCache` 恒为 false | 三处类型判断、清单变量和测试矩阵；3 个生产分支检查恒定开关 | 删除伪选择器（配置契约变更）和 `noCache` |
| O36 | P3 | 单一事件 worker 经接口族、切片和类型断言调度；Controller / Scheduler 维护两套任期容器（B2） | 4 处类型断言；`errChan` 无读者；两套 begin/stop | 直接持有 `*workflow.Workflow`，再评估单一任期容器 |
| O37 | P3，待决策 | 延迟通知 topic 与数据库到期扫描是同一入口的两套实现 | broker 读取、认领、ACK、去重，以及 topic 预建和健康项 | 若不需要亚 3 秒精度，只保留数据库扫描 |
| O38 | P3 | 版本更新的组件变更被解析 17 次，存在性校验有 5 份 | 规格有三套表示，replicas 默认值写在两处 | 校验后一次性生成类型化变更列表 |
| O39 | P3 | 清理标记 JSON 结构有 5 份定义，匹配、模板归一和重试集合各有两份 | 改一个字段要同步 5 处；`omitempty` 不一致 | 沿用 O18 的做法收敛到 `domain/model`；字节格式不变 |
| O40 | P3 | 旧持久化格式同时由启动迁移和运行时回退处理 | 同一条兼容规则有两个执行点 | 保留迁移则删除运行时推断；涉及存量数据假设 |
| O41 | P3 | repository 接口与包级函数两套 API 并存，接口写方法多数无生产调用 | 测试替身要实现空方法 | 删除无调用方法，消费者改用只读窄接口 |
| O42 | P3 | Pod coordinator 的 observe 事件机制没有生产入口 | 额外的锁、状态和删除处理；相关函数覆盖率为 0% | 删除 observe 状态和两个无用 option |
| O43 | P3 | HTTP 路由策略、gRPC 方法策略、路由到 RPC 映射三张表并存 | 新增接口要同步 3–4 处；角色漂移没有测试 | 由映射推导 gRPC 角色，或补一致性测试 |
| O44 | P3 | 中间件有无生产用途的选项，健康检查路径有 5 份副本 | 并发限流中间件无调用；RateLimit / CORS 回退不可达 | 删除未接入的中间件和不可达回退，路径集中定义 |
| O45 | P3 | 4 个校验 tag、12 个错误码和导出映射副本没有生产使用 | 文档列出永远不会返回的 10012 / 10014 | 删除，并同步修正文档 |
| O46 | P3，待决策 | Harbor Runner 保留没有 `sandboxURL` 的直连 Pod 路径 | 主测试矩阵跑的是旧路径 | 需要先撤销 Current 文档中的共存承诺 |
| O47 | P3 | installer 保留旧安装迁移代码和伪选项 | 删除旧 cluster-admin binding、旧 Secret 回退、无效变量 | 删除，并在文档写明不支持旧安装 |
| B1 | 待评估 | 结果收尾仍经过同一 Leader 内的 broker 往返 | 两个额外持久化状态，两套恢复机制 | 作为独立的恢复协议 PR 评估 |

## 3. 范围与证据强度

审查从 `docs/README.md` 的需求定位出发，按以下 5 个范围并行检索。每项发现都由审查者回到基线代码复核，用 `rg --glob '!*_test.go'` 确认生产调用方，未经复核的线索不列为发现。

| 范围 | 深度 | 结果 |
| --- | --- | --- |
| 启动、选举、配置、infrastructure、部署 | 直接阅读装配、配置校验、缓存、datastore、informer、installer | O33、O35、O36、O47 |
| Workflow 引擎、Job、CloudJob | 深读派发、延迟、结果 outbox、Job 删除与清理、运行配置 | O32、O34、O37、B1 |
| 应用领域服务、校验、转换 | 深读 `WorkflowService`、版本更新、步骤校验、清理重试 | O29、O30、O31、O38、O39 |
| 资源导入、model、repository、spec、account | 深读 Pod coordinator、repository 接口、迁移与回退 | O40、O41、O42 |
| HTTP/gRPC、Jobs/Harbor、Traits | 深读中间件、授权表、校验器、错误码、Runner 协议 | O43–O46 |

以下内容没有审查或验收：

- 生成的 Protobuf 绑定和第三方依赖没有逐行审查；
- 同步 namespace import 的 observe/try 路径只粗略估计了规模；
- 没有执行真实 MySQL、Redis、Kafka、Kubernetes、ACS 验收，也没有做负载测试；
- 没有调查仓外的 Go 消费者。

## 4. 与正确性相关的冗余（P2）

### O29｜`WorkflowService` 的死方法族带走了延迟任务的派发 fence

**证据。**

- `domain/service/workflow/workflow.go:38-57` 定义了 `WorkflowService`。以下方法在非测试代码中只出现在接口声明里：`CreateWorkflowTask`（97）、`ExecWorkflowTask`（171）、`UpdateTask`（942）、`MarkTaskStatus`（951）和 `TaskRunning`（1015）。
- `CreateWorkflowTask` 还连带了 `ConvertWorkflow`（144）和 `workflow_validation.go:14` 的 `lintWorkflow`。
- `event/workflow/workflow.go:35-40` 的运行时接口也声明了 `UpdateTask` 和 `MarkTaskStatus`，但生产代码只调用 `WaitingTasks` 和 `DispatchWorkflowSchedules`。

**被掩盖的行为。** `MarkTaskStatus` 遇到 Waiting→Queued 时，会转入 `claimWaitingTaskForDispatch`（958-1012）。这段代码对到期的延迟任务做三件事：加应用锁、复核 app ID、对非迁移任务调用 `EnsureNoPendingStatefulSetCleanup`。它的注释写明“只有延迟任务会越过入队时的安全判断”。

但真实派发路径是 `event/workflow/dispatcher.go:247-249` → `domain/repository/workflow_lease.go:28-70` 的 `ClaimWorkflowTaskForDispatch`，这条路径只做状态和代次的 CAS。另一方面，`EnsureAppWorkflowIdle`（2186 起）不把未到期、也没有活跃 Job 的 Waiting 任务算作活跃任务。

由此推断出一个可能的场景：

1. 普通延迟任务入队；
2. 随后提交的 StatefulSet 迁移失败，留下 pending cleanup；
3. 延迟任务到期后照常派发，不经过 fence。

`statefulset_cleanup_fence_test.go:374-450` 的 4 个 `TestMarkTaskStatus*` 测试在基线上全部通过，但它们只证明死路径正确。本轮**没有端到端复现**，也没有确认执行期是否存在其他保护。

**同类问题。** `ApplicationsService.GetApplication`（`domain/service/application/application_query.go:454`）和 `DeleteApplication`（570）也没有生产调用。HTTP 和 gRPC 的删除都走 `DeleteApplicationCascade`（`application_delete.go:24`）。`DeleteApplication` 不加锁、也不取消任务，以后被误用时会绕过级联保护。

**最小简化。**

1. 先决定延迟任务在派发时是否需要 fence。需要的话，把检查接到真实派发之前，并保持立即执行和 Cron 任务不依赖 locker；
2. 再删除上述方法、连带的 DTO 和校验，以及运行时接口里的多余方法；
3. 测试改到真实派发路径上。

**必须保留：** 派发 CAS、代次与 token、迁移任务对 fence 的豁免、入队时已有的 fence。

**契约：** 只影响导出的 Go 符号；HTTP 和持久化不变。

### O30｜Workflow 步骤校验在写入和 Try 各有一套，已经分歧

**证据。**

- 写入路径：`domain/service/application/application_workflow_steps.go:458-703`，调用点在 `application.go:285` 和 `1055`。
- Try 路径：`domain/service/validation/validation_workflow.go:115-252` 和 `276-511`。
- 两边各自实现了审批（`steps.go:680` 对应 `validation_workflow.go:461`）、log archive（`632-661` 对应 `433`）和 properties 规则。

已经出现的分歧：

- Try 对非法 mode 返回 `ErrCodeInvalidWorkflowMode`（`validation_workflow.go:149`）。写入路径 `steps.go:203` 用 `config.ParseWorkflowMode`（`config/consts.go:247-256`）把未知值静默当成 StepByStep。
- stepType 同理：写入路径调用 `ParseWorkflowStepType`，默认当作 component。
- 步骤重名只在 Try 中拒绝。

**额外成本。** 同一条规则要在两种错误模型（逐项 `ValidationError` 与首个 `bcode` 错误）里分别维护。O08 只收敛了 Trait 的叶子规则，没有覆盖步骤规则。

**最小简化。** 沿用 O08 的做法：把每条规则抽成返回字段、错误码和消息的叶子函数；Try 汇总全部错误，写入只取第一个。

**契约：** 写入路径采纳 Try 的规则后会拒绝过去被静默接受的请求，属于 HTTP 行为变更，需要写迁移说明。

### O31｜版本更新 `strategy` 只是一个没有行为的枚举

**证据。**

- `domain/spec/application_policy.go:27-55` 定义了 rolling、recreate、canary、blue-green 四个值；未知值静默当作 rolling。
- 解析结果只在 `application_update_version_phases.go:66` 保存，并在 357 回显到响应、在 384 写入审计日志，不影响任何资源生成。
- 实际的滚动方式由 `traits.rollout` 决定。
- `bcode.ErrInvalidUpdateStrategy`（`utils/bcode/001_application.go:37`）从未被引用。
- 字段同时暴露在 DTO（`interfaces/api/dto/v1/types_version.go:14,57,104`）和 proto（`applications.proto:575,924,939`）中。
- `docs/version-update-api.md:17-25` 列出了 recreate、canary、blue-green 的行为说明，`485-500` 还给出了“金丝雀发布”示例。

**额外成本。** Current 文档承诺了一个不存在的能力，调用方无法从响应判断请求的策略是否生效。

**最小简化。** 只接受空值或 `rolling`，其他值用现有错误码拒绝；删除三个未实现的常量；修正文档和示例。如需保留字段供以后扩展，也必须拒绝未实现的值。

**契约：** HTTP 和 gRPC 请求校验变严；本轮没有调查仓外客户端是否发送过这些值。

### O32｜batch Job 删除的多份实现中，三处没有指定传播策略

**证据。**

以下 3 处没有设置 `PropagationPolicy`：

- `event/workflow/job/job_instant.go:93-95` 和 `job_scheduled.go:49-51` 的失败清理用空的 `metav1.DeleteOptions{}`；
- `job_batch.go:257-266` 的 Recreate 只设置了 UID 前置条件。

其他删除点各自手写策略：

- `job_pod_logs.go:233`：Background；
- `cancel_recovery.go:346,363`、`job_retry.go:669`：Foreground；
- `job_cleanup_resources_labeled.go:54`：Background；
- `job_cleanup_resources_statefulset_pods.go:200`：有意使用 Orphan。

“等待 Job UID 消失”的函数也有三份：`job_pod_logs.go:263`、`job_batch.go:709` 和 `cancel_recovery.go:378`。

**额外成本与风险。** 前置条件、传播策略和等待逻辑每处各写一遍，策略差异只能逐点比对。按 Kubernetes `batch/v1` Job 在 API 删除时默认 orphan 子 Pod 的语义，上述三处在超时或失败后删除 Job，可能留下仍在运行的 Pod；Recreate 还可能让旧 Pod 和新 Job 并存。fake client 不模拟 GC，本轮**没有在真实集群验证**。

**最小简化。** 收敛为一个按 UID 前置条件删除并显式传入策略的函数，加一个等待函数。各调用点保留自己的策略选择；StatefulSet owner Job 的 Orphan 是刻意的例外。

**必须保留：** UID 和 resourceVersion 前置条件、先收集日志再清理、清理失败保留现场。

**契约：** HTTP 和持久化不变；这三处会变成级联删除 Pod。

## 5. 运行时装配与配置

### O33｜DataStore 的“可选能力”接口只有一种生产实现

**证据。**

- `infrastructure/datastore/datastore.go:200-233` 定义了 `ConditionalCompareAndSwap`、`DatabaseClock`、`Transactional`、`ReadCommittedTransactional`、`RowLocker` 五个可选接口。
- 生产链路固定为 MySQL `sql.Driver`（`infrastructure/datastore/sql/driver.go:25,416`、`transaction.go:15-30`）外包一层 `account.Store`（`domain/service/account/access_store.go:321-464`），两者都实现了全部五种能力。
- 尽管如此，非测试代码中仍有 27 个文件、63 处在运行时断言这些能力。

降级分支在生产中走不到：

- `domain/repository/workflow_lease.go:243-254` 和 `504-512` 在缺少 ReadCommitted 时退回默认隔离级别；
- `domain/service/application/application.go:332-344` 和 `domain/service/workflow/workflow.go:1484-1487` 在缺少事务时直接以非事务方式执行；
- Jobs 侧还有 A11 的残留：`jobs/artifacts/store.go:51-59` 每个事务都重新断言一次，`jobs/builder.go:267-270` 运行时断言 `DatabaseClock`。

**额外成本。** 每个调用点都要写“断言 + 不支持时报错或降级”，测试替身也要分别实现或拒绝各项能力。更重要的是，“无事务也继续”这类降级分支语义很弱，一旦以后有新存储实现落入其中，就会悄悄失去原子性。

**最小简化。** 把五种能力并入 `DataStore`（或一个必需的组合接口），删除 `!ok` 分支和降级执行；sqlite 测试方言照常实现同一接口。

**必须保留：** CAS/fencing、数据库时钟、READ COMMITTED 事务和 `FOR UPDATE` 的语义。

**契约：** 只影响导出的 Go 接口。

### O34｜Workflow 运行配置：无入口字段、矛盾默认值和超时双表示

**证据。**

- `workflow/config/runtime.go:33-42` 的 `WorkerMaxReadFailures`、`WorkerMaxClaimFailures`、`WorkerBackoffMin`、`WorkerBackoffMax` 没有对应的 flag 或 env（`config/config.go:350-364`）。配置只来自 flags/env，`config/apiserver-default.yaml` 只是参考文件。
- 生产中两个失败上限恒为 0（`runtime.go:88-89`），所以 `event/workflow/dispatcher.go:187-190` 和 `224-227` 的 worker 退出分支不可达。
- 在 `Cfg == nil` 时，`event/workflow/workflow_state.go:66-78` 却回退到 10（`runtime.go:71-72`），和生产语义相反。
- 另有 13 个访问器各自再做一次 `> 0` 回退，与 `RuntimeConfig.Validate`（`runtime.go:101`）重复。

回调超时上限也有两种表示：

- `event/workflow/job/job_callback.go:66-67` 同时有 `TimeoutMaxSec` 和 `TimeoutMaxNS`；
- 三个生产构造点（`event/workflow/controller.go:1172-1173,1424-1425`、`domain/service/workflow/workflow.go:1746-1747`）用同一个值各写一份；
- 读取端 `job_callback.go:273-281` 优先使用 NS，Sec 分支不可达；
- `JobTask` 只是内存载荷（`domain/model/job.go:50-62`），不会被序列化。

**附带缺陷（单位）。** `config/consts.go:141` 定义 `DefaultJobTaskTimeout = 20 * time.Minute`。但 `event/workflow/controller.go:857` 和 `event/workflow/job_builder_component.go:29` 用 `int64(config.DefaultJobTaskTimeout)` 作为“秒数”回退值，得到的是 1.2×10¹² 秒。

- 前者在 `--workflow-default-job-timeout` 取 (0, 1s) 时可达：`Validate` 只要求 > 0。
- 后者需要调用方传入 ≤0，当前生产调用方不会这样做。

触发条件很少，但它正是“同一默认值有 Duration 和秒两种表示”的直接后果。

**最小简化。** 删除这 4 个字段和对应的退出分支；访问器直接读取已校验的配置；默认 Job 超时只保留一个 Duration，到最终写入 Kubernetes 时再换算为秒；删除 `TimeoutMaxSec`。

**契约：** flags/env 不变，只影响导出的 Go 字段和常量。

### O35｜只有一个合法值的后端选择器与恒定缓存开关

**证据。** 以下为历史遗留项，当前仍存在。

`datastore-type`：

- 字段、默认值、flag 和校验分布在 `config/config.go:152`、`196-197` 和 `322`；
- `server_assembly.go:80-92` 用字面量 `case "mysql"` 再判断一次，`schema.go:15-16` 第三次判断；
- `config/consts.go:13` 的 `TIDB` 没有任何引用。

`cache-type`：

- 定义和校验在 `config/config.go:97`、`164`、`222-224` 和 `342`；
- 部署清单 `deploy/eruun-stack.yaml:382,391` 和 Helm `runtime-deployments.yaml:72-73` 固定写入；
- `README.md:216` 说明它只接受 `redis`。

缓存开关：

- `infrastructure/cache/icache.go:12-18` 的 `IsCacheDisabled()` 在生产中恒为 false：唯一的构造调用 `server_assembly.go:124` 传入 `noCache=false`；
- 尽管如此，`event/workflow/job/job.go:983`、`domain/service/application/application_cache.go:28` 和 `component_status_sync.go:49` 仍在检查它；
- `MemCache`（`mem_cache.go:91`）没有生产调用方。

**额外成本。** 三处后端判断、两份清单变量和配置测试矩阵，表达的是一个不能更改的事实；恒定开关则让读者误以为存在关闭缓存的模式。

**最小简化。**

- 删除两个 flag、两个字段、`TIDB` 常量和多余分支，Redis 地址校验只保留一处；
- 删除 `noCache` 和 `IsCacheDisabled`，`MemCache` 移到测试 helper。

**契约：** 移除 `ERUUN_DATASTORE_TYPE` 和 `ERUUN_CACHE_TYPE` 属于配置契约变更。按 O20 的先例，旧变量应当被明确拒绝，而不是静默忽略。同时要更新清单、Helm、README 和本地依赖文档。

### O36｜单一事件 worker 的接口族与两套任期容器（含 B2）

**证据。**

- `event/event.go:9-29` 定义了三个接口，但 `InitEvent` 只返回只含一个 `*workflow.Workflow` 的切片。
- 服务端随后要对切片循环并做类型断言：
  - 回到具体类型：`server_assembly.go:352`、`server_runtime_metrics.go:28`；
  - 回到接口：`server_workers.go:264`、`281`；
  - 为单元素切片做 nil 过滤和就绪计数：`server_workers.go:123-129`、`275`。
- `StartController`、`StartScheduler`、`StartWorker` 的 `errChan` 参数（`event/workflow/workflow.go:101,120`、`dispatcher.go:129`）都没有被读取，`Workflow.errChan` 字段（`workflow.go:61`）也无人读写。
- B2 仍然存在：`server_workers.go:302-362` 分别维护 `controllerRun` 和 `schedulerRun` 的 begin/stop。生产中由统一选举循环串行调用，每个任期结束时 `stopLeader`（`server_runtime_leader.go:106-127`）会先停止两个容器，因此 begin 中处理“上一个 run”的分支在当前路径上走不到，只由测试保活。

**最小简化。** `restServer` 直接持有 `*workflow.Workflow`；为测试替换保留一个窄接口即可；删除未读的 `errChan` 参数和字段。B2 应在此之后单独评估：用一个任期容器管理两类循环，但保留 Informer 先停后启、Controller 启动失败不启动 Scheduler、Scheduler 就绪屏障，以及释放 Lease 前所有循环退出。

**契约：** 只影响导出的 Go 符号。

## 6. 消息与恢复协议

### O37｜延迟通知 topic 与数据库到期扫描是同一入口的两套实现（待决策）

**证据。**

- 生产者先持久化检查点，再发送通知；通知失败只记录日志：`event/workflow/job/job_instant.go:213-218`、`job_scheduled.go:178-183`。
- 消费者允许队列不存在（`delay_dispatcher.go:119-122`），并且每 3 秒（`workflow/config/runtime.go:57`）独立扫描数据库恢复到期检查点（`delay_dispatcher.go:296-369`）。
- 只为 broker 存在的代码包括：读取和认领循环（170-249）、通知解码（382-404）、ACK（493）和 `delay_queue.go` 的入队。
- 设计文档 `docs/enterprise-distributed-runtime-design.md:78` 写明：Redis Stream “只负责降低到期发现延迟”。

**额外成本。** 两个入口汇入同一个内存堆，靠 pending map 去重；未到期的消息长期不 ACK，会被周期性认领后再放回。此外还要维护 topic 预建、消费组、健康检查项和对应测试。

**决策点与边界。** 如果产品不要求延迟任务有亚 3 秒的触发精度，可以只保留数据库扫描。必须保留：检查点先落库、准入、workspace 校验，以及先建 outbox 再标记 dispatched。这会删除 delay topic 和消费组、健康 JSON 中的 delay 项，以及导出的 `EnqueueDelayJob`；持久化不变。

### B1（复核）｜结果收尾仍在同一 Leader 内经过 broker 往返

`event/workflow/workflow.go:108-110` 在同一个 `StartController` 中同时启动延迟分发器、结果消费者和 outbox 发布器。链路如下：

1. outbox 发布器把 pending 改为 dispatching，入队后再改为 queued（`event/workflow/job/job_result_outbox.go:168-226`），并负责过期恢复（113-151）；
2. 同一进程的消费者执行 Read 和 AutoClaim（`job_result.go:122-213`）；
3. 消费者在数据库中认领结果（327-371）。

在这条链路中，broker 不提供持久性（数据库才是恢复来源），也不做跨节点分发（唯一的消费者就在 Leader 上）。由此带来的额外成本：

- `result_dispatching_queue` 和 `result_queued` 两个持久化状态（`config/consts.go:161-165`）；
- 消息 ID 兼作处理 token；
- AutoClaim 与数据库租约两套恢复机制并存。

结论维持“收益可能较大，但不能直接删除”：改为有界处理池直接在数据库认领 pending，需要证明认领公平性、索引与扫描成本、背压、长任务占槽、切主接管和失败重试。子代理推导出“16 个槽位全被长任务占满时，queued 记录会在租约到期后反复回到 pending”的场景，**尚未实测**，应作为评估时的首个验证用例。

## 7. 领域服务与持久化

### O38｜版本更新的组件变更被重复解析与重复校验

**证据。**

- `domain/service/application/application_update_version_actions.go` 已经把 restart 动作拆出，其余组件放进 `normalReq.Components`。
- 之后非测试代码仍在 11 个文件中调用 `parseVersionUpdateComponentAction` 17 次，contract、cleanup、pvc、image_ready、apply、workflow、statefulset 等阶段各自重新解析。
- “组件不存在 / 已存在”的检查有 5 份；冲突校验在 `application_update_version_phases.go:101` 和 `166` 各跑一次，后一次的输入是前一次的子集。
- 组件规格有三套表示：解析后的更新规格、存储模型、只用于判断“是否有变化”的比较结构。replicas 默认值 1 写在两处。

**最小简化。** 校验之后一次性生成类型化的变更列表（action、key、spec、当前组件），各阶段只消费这份列表，落库时由已解析的结果转换成模型。

**必须保留：** 第一个错误的错误码与顺序、adopted 和 shared 限制、snapshot 校验、没有变化时不落库。

**契约：** 无。本轮没有逐一证明三套表示在 `NormalizeComponentEvaluation` 之后完全等价，实施时要先补等价性测试。

### O39｜清理标记的持久化结构有 5 份定义

**证据。** `{source, version, requireStatefulSetDeletion, statefulSetPVCTemplatesToDelete}` 这组 InternalInfo 字段分别定义在以下 5 处：

- `domain/model/workflow_queue.go:151-152`；
- `domain/service/application/application_update_version_cleanup_retry.go:39-40`；
- `domain/service/workflow/statefulset_cleanup_fence.go:21-22`；
- `event/workflow/job_builder_cleanup.go:292-293`；
- `event/workflow/job/job_info.go:461-462`。

后两处写入时不带 `omitempty`，读取端却带。

另外三组逻辑各有两份：

- 可重试终态集合：`cleanup_retry.go:626-633` 与 `statefulset_cleanup_fence.go:319-326`，内容完全相同；
- 按组件匹配清理 Job：`cleanup_retry.go:713` 与 `fence.go:406`；
- 模板名归一与比较：`cleanup_retry.go:806-840` 与 `fence.go:462-489`。

**最小简化。** 沿用 O18 的做法，把标记类型、编解码、模板归一和重试集合集中到 `domain/model`，各调用方继续自己判断 identity、成功与否和覆盖范围。

**必须保留：** 现有 JSON 字节格式、v2/v3 校验、重复 Job 拒绝、PVC 删除保护。

**契约：** 持久化格式不变。

### O40｜旧持久化格式由“启动迁移 + 运行时回退”双轨处理

**证据。** 同一条兼容规则在启动迁移和运行时各执行一次：

- **management_mode**：`infrastructure/datastore/mysql/migrations.go:78-125` 在启动时把空值迁移为显式模式；`domain/model/applications.go:81-96` 在运行时仍然对空值按 `imported/imported` 推断为 observe。当前唯一的创建入口已经显式写入模式。
- **资源创建预算 interval**：`migrations.go:127-183` 迁移，`domain/repository/resource_creation_budget.go:50-52` 在运行时也处理未知 interval。
- **旧配置表、旧列和 NULL 回填**：`migrations.go:191-282`。
- **导入 snapshot v1**：`domain/service/resourceimport/contract/snapshot.go:20` 保留 `legacySnapshotVersion`，所有生产写入方都已经写 v2。

**额外成本。** 同一规则有两个执行点和两套测试矩阵；新读者很难判断哪一处才是权威。

**最小简化。** 对每条规则选定一个执行点：如果迁移要保留，就删除运行时推断；如果开发期不再支持旧库，就连迁移一起删除，并让旧数据明确失败。

**必须保留：** AutoMigrate、迁移锁与 marker、“未知非空模式按 observe 处理”的只读保护。

**契约：** 涉及存量数据假设，要同步 `docs/application-management-mode.md`。本轮没有确认是否还有开发库依赖这些迁移。

### O41｜repository 接口与包级函数两套 API

**证据。**

- `domain/repository/application.go:13-22`、`workflow.go:19-26` 和 `66-74` 定义接口，`application.go:94` 注明包级函数“kept for backward compatibility”。
- 生产代码中，接口写方法只有 `WorkflowRepo.Create/Update`（`domain/service/application/application.go:1115,1132`）和 `AppRepo.Delete` 有调用，而后者只在 O29 列出的死方法 `DeleteApplication` 中。
- 其余写方法都没有生产调用：
  - `ApplicationRepository.Create/Update`；
  - `WorkflowRepository.Delete/DeleteByAppID`；
  - `ComponentRepository.Create/Update/Delete/BatchAdd/DeleteByAppID`。
- 事务内的写入实际都直接调用包级函数。

**额外成本。** 多个包的测试替身要实现这些空方法，两套 API 的选择也没有规则可循。

**最小简化。** 删除无调用的方法；resourceimport、validation 等消费者改用只包含 `FindByID`、`List`、`FindByAppID` 等方法的只读窄接口。

**契约：** 只影响导出的 Go 接口。

### O42｜Pod coordinator 的 observe 事件机制没有生产入口

**证据。**

- `server_workers.go:378` 创建 coordinator 时没有传任何 option，所以 `WithPodObservationFunc`（`domain/service/resourceimport/runtime/pod_coordinator.go:139`）在生产中从未被调用。
- `binding_loader.go:62-67` 只给 adopted 应用填写 workload UID，因此 observe binding 不满足 `valid()`（`pod_coordinator.go:55-59`），也不会进入观察集合。
- 结果，以下状态和逻辑都只能在测试中触发：
  - `observedPods`、`observeMu` 状态（83-110）；
  - 只读分支（531-538）；
  - `updateObservedPod`、`deleteObservedPod`、`notifyObservedPod`、`syntheticObservedPodFromSource`（575-636）；
  - 重载时的清理（289-300）。
- 基线覆盖率：`updateObservedPod`、`notifyObservedPod`、`syntheticObservedPodFromSource`、`handleDeletedPod` 均为 0%。
- `WithMaxQueueItems` 没有调用，`WithBindingReloadInterval` 只在测试中使用。

**最小简化。** 删除 observe 状态、相关函数和两个无用 option，并评估 `handleDeletedPod` 是否仍有必要。

**必须保留：** owner UID 链校验、带 UID/resourceVersion 前置条件的 JSONPatch、标签声明冲突检测、有界队列和 generation。

**契约：** 只影响导出的 Go 符号。

## 8. 接口层

### O43｜HTTP 与 gRPC 的授权和限流分类维护了三套表

**证据。** 同一份“接口 → 最低角色”的映射分三处维护：

- `interfaces/api/middleware/auth.go:47-123` 维护“路由 → 角色”；
- `interfaces/grpc/server.go:60-140` 的三张 `*MethodPolicies` 维护“RPC → 角色”；
- `interfaces/grpc/coverage.go:6-106` 维护“路由 → RPC”。

前两张表的内容可以由第三张推导出来。`coverage_test.go` 只检查路由和 RPC 是否都存在，不比较两边的角色。

“昂贵请求”的判定也有两份：`grpc/server.go:320-332` 与 `middleware/rate_limit.go:69-81`。

**额外成本。** 每新增一个接口要同步 3–4 处；HTTP 与 gRPC 的角色出现漂移时，没有任何测试会失败。

**最小简化。** gRPC 在启动时通过 `routeRPC` 反查 HTTP 的角色，`system` 类路由按现有规则映射为 `system_workspace`。如果暂时不改装配，至少补一个“同一接口两边角色相同”的一致性测试。

**必须保留：** 未知方法默认拒绝、强制改密门禁、Bearer 认证与空间授权。

**契约：** 无。

### O44｜中间件的无生产用途选项与健康路径副本

**证据。**

- `interfaces/api/middleware/global_concurrency_limit.go:10-47` 没有任何生产调用。
- `RateLimit` 的 `QPS/Burst` 回退（`rate_limit.go:33-41`）不可达，因为 `server_bootstrap.go:61-69` 总是传入 `Shared`。倍数常量（`rate_limit.go:11`）只有测试使用，实际倍数写死在 `interfaces/ratelimit` 中。
- CORS 的通配符分支（`cors.go:29,43,49-54`）不可达，因为 `domain/spec/accounts.go:114-118` 只接受精确的 HTTPS origin。
- 健康检查路径列表有 5 份：`auth.go:23-25`、`rate_limit.go:21-29`、`tracing.go:51-52`、`server_leader_api.go:40`、`grpc/coverage.go:121-124`。其中 `tracing.go` 中不带前缀的 `/health` 等路径永远匹配不到。
- `/api/v1/health`、`/api/v1/ready`（`health.go:40,42`）是 `healthz/readyz` 的别名。部署清单和 Helm 探针只使用后者，文档也没有记录这两个别名。

**最小简化。**

- 删除未接入的并发限流中间件；
- `RateLimit` 只接收共享 limiter；
- CORS 只保留精确 origin 加 credentials 的路径；
- 健康路径收敛为一个常量。

**契约：** 删除两个健康别名属于 HTTP 路由变更（未记录在文档中）；其余不影响契约。

### O45｜定义了但没有生产使用的校验 tag、错误码与导出副本

**证据。**

- `interfaces/api/validate.go:42-64` 注册了 5 个校验 tag，DTO 只使用 `checkname`。`checkalias`、`checkemail`、`checkpassword`、`checkMode` 和 `emailRegexp` 没有任何引用。
- `init()` 与 `InitValidator`（经 `runtime.go:12`）重复触发同一次注册。
- `utils/bcode/001_application.go` 中有 11 个错误码从未被返回：19、37、43、54、57、60、63、66、69、72、75 行，另有 `bcode.go:105` 的 `ErrNotImplemented`。其中 10012 和 10014 仍被 `docs/version-update-api.md:199,201` 列为可能的返回值。
- `grpc/coverage.go:127-134` 的 `RouteRPCMappings` 没有调用。

**最小简化。** 删除未用的校验器和错误码；修正文档的错误码表；删除导出的映射副本。

**必须保留：** `checkname`、错误码重号检测。

**契约：** 删除导出的 Go 符号，修正文档；HTTP 行为不变。

## 9. Jobs 与部署

### O46｜Harbor Runner 保留没有 `sandboxURL` 的直连 Pod 路径（待决策）

**证据。**

- 服务端 `jobs/builder.go:194-199` 每次都写入 `sandboxURL`、`options` 和两个超时字段。
- Runner 仍允许缺少 `sandboxURL`：
  - `jobs/runners/harbor/runner.py:88-89`；
  - `runner.py:268-294` 中 `pod_overrides` 和 ownerReferences 只服务直连 Pod 路径；
  - `eruun_environment.py:293-295` 在没有 sandbox control 时走 `super().start`。
- 测试的默认配置大多不带 `sandboxURL`，主测试矩阵跑的反而是旧路径。

**决策点。** `docs/workspace-jobs-api.md:217` 和 Runner README 第 71 行把这条路径写成升级共存承诺，所以不能直接删除。如果确认开发阶段不支持新旧 Runner 共存：

- 把 `sandboxURL` 和两个超时设为必填，删除直连 Pod 分支；
- 测试夹具改用 Sandbox mock；
- 同步修改两份文档。

Runner ServiceAccount 的 Pod 创建权限是否只服务旧路径，本轮**未验证**。

### O47｜installer 的旧安装迁移代码与伪选项

**证据。** 以下为历史遗留项，当前仍存在。

- `deploy/all_in_one_install_quickstart.sh:492-503` 删除旧的 `eruun-platform-cluster-admin` binding，并在第 550 行无条件调用。
- 第 253-256 行在旧 Helm Secret 缺少 `database` key 时，回退到读取 StatefulSet 的环境变量。
- `REDIS_WORKLOAD_NAME`（48）从未被读取。
- `REDIS_WORKLOAD_KIND`（47、320-323、518）接受 deployment，但清单固定是 StatefulSet，选 deployment 只会让 rollout 等待失败。

**最小简化。** 删除这两段迁移代码和两个伪选项；在安装文档中写明不支持从旧安装原地升级。

**必须保留：** 持久化凭据复用、占位符拒绝、PVC 存在而 Secret 缺失时失败。

**契约：** 影响 installer 环境变量；旧安装需要人工迁移。要更新 `deploy/all_in_one_install_quickstart_test.sh`。

## 10. 低优先级清理（随邻近修改处理）

以下各项都已确认没有生产调用或属于只写状态，但单独立项的收益不足：

- **artifacts 的变长参数**：`jobs/artifacts/store.go:88-110` 等 11 个签名把 `executionKey ...string` 当可选参数，多余的值被静默忽略。`PutResult`（183）只有测试使用。可改为显式的 `executionKey string`，但要保留空 key 时的 ID 公式。
- **服务端自注入与只写字段**：`server_assembly.go:54` 把 `restServer` 作为 bean 注入容器，但没有任何 `inject:"RestServer"` 消费方。`restServer.KubeConfig` 没有读者，`runtimeQueues`（`server.go:54`）只写不读。
- **informer 只被测试使用的 API**：`infrastructure/informer` 中无任期版本的 `OnPodAdd/OnPodUpdate/OnPodDelete`、`ResetPodSnapshots`、`GetStats`、`WithSyncTimeout` 没有生产调用。
- **CloudJob 注册表**：`CloudProvider.SupportedActions` 没有生产消费方；`event/workflow/job/cloud_provider_bridge.go:34` 的 `RegisterCloudProvider` 转发也没有调用方。注册表本身服务于文档化的源码接入模板，予以保留。
- **死模型字段与表**：
  - `domain/model/system_info.go` 注册后会建表，但没有读写；
  - `JobInfo.Production/TargetEnv`（`domain/model/job.go:31-32`）从不被设为非零值，却经 gRPC 对外输出。删除涉及 schema 和 proto，应与 O40 一起处理。
- **纯转发函数**：`domain/service/application/workflow_idle.go:14-40` 的 7 个函数只是转发到 workflow 包，调用方可以直接使用目标导出函数。

## 11. 不列为过度设计的复杂性

- `JobCtl` 有 20 多个真实实现；`ResourceImportExecutor` 是跨模块的消费方接口。
- 顺序执行与 Pool 两条 RunJobs 路径的失败语义不同。
- Redis 取消信号与数据库租约并存属于延迟权衡，数据库是权威来源。
- adopted 清理、共享扫描、锁外预检加事务内复检、模板克隆改写都对应真实需求。
- resourceimport 的 `contract` 子包有 20 多个跨包消费者，用于避免循环依赖；`runtime` 子包隔离了只在 Leader 上运行的协调器。
- `handler_result.go` 的泛型 helper 有超过 50 处生产调用；`json_schema.go` 通过反射从 Go 类型生成 Current schema。
- 已删除配置的明确拒绝（例如 `ERUUN_AUTO_TRACING`）是记录在案的 fail-fast 迁移决策，不是兼容别名。
- `DatabaseResetResponse.RestartComponents` 恒为空，但已在 `docs/database-reset-workflow.md` 中声明为兼容字段，属于 Current 契约。

## 12. 历史遗留项状态

| 来源 | 项目 | 基线状态 |
| --- | --- | --- |
| [Leader/Worker 复杂度审核](leader-worker-complexity-audit-2026-10-07.md) | B1 结果 broker 往返 | 仍存在，见本文 B1 |
| 同上 | B2 两套任期运行容器 | 仍存在，并入 O36 |
| 同上 | `datastore-type` / `cache-type` 伪选择器 | 仍存在，并入 O35 |
| 同上 | installer 删除旧 cluster-admin binding | 仍存在，并入 O47 |
| [2026-09-26 代码质量审计](code-quality-audit-2026-09-26.md) | A07 全局 API registry 保存可变 handler | 已处置：`interfaces/api/interfaces.go:17-29` 为每个 server 新建 handler |
| 同上 | A08 Trait 字符串、反射调度 | 已处置：`workflow/traits/processor.go` 为显式类型调度 |
| 同上 | A11 Jobs 必需存储能力靠可选接口断言 | 部分残留，并入 O33 |

## 13. 建议整改顺序

整改按回滚边界分组，每组各自成为独立 PR，不建议一项一个 PR。

1. **行为风险先行**：O29 的 fence 决策与接线、O32 的 Job 删除策略、O34 的单位缺陷。每项都要先写出失败场景的测试。
2. **纯删除，低风险**：O36（不含 B2）、O41、O42、O44（不含健康别名）、O45，以及第 10 节的低优先级清理。
3. **契约变更，需要迁移说明**：O30、O31、O35、O40、O44 的健康别名、O47。
4. **需要设计验证**：O33、O38、O39、O43、B2，以及待决策的 O37、O46 和 B1。

同步 namespace import 的 observe/try 路径只服务旧客户端，规模粗估在千行以上，但它是公开路由，需要产品先确认旧客户端是否仍然存在，本文不列为整改项。

## 14. 验证记录

使用 Go 1.27.1，在基线 `fc37007` 上执行：

```sh
go test ./pkg/apiserver/domain/service/workflow -run '^TestMarkTaskStatus' -count=1 -v
go test ./pkg/apiserver/domain/service/resourceimport/runtime/ -count=1 -coverprofile=/tmp/eruun-audit-runtime.cover
go tool cover -func=/tmp/eruun-audit-runtime.cover
```

- 4 个 `TestMarkTaskStatus*` 测试全部通过，说明 fence 只在 O29 的死路径上被测试。
- runtime 包的 observe 相关函数覆盖率如 O42 所述。

生产调用关系使用 `rg --glob '!*_test.go'` 检索，并人工复核每个调用点。本 PR 只新增本文档并更新文档索引，没有修改源码，所以没有运行全仓 race、vet、build、Helm 或安装测试。

以下事项**未验证**：O29 的端到端触发、O32 的真实集群 GC 行为、B1 的饱和场景、仓外 Go 消费者和客户端对 `strategy` 的使用。
