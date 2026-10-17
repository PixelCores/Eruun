# 过度设计审计（2026-10-08）

> 状态：Historical / Audit。原始审查基线为 `main@fc37007d9bb10d355015c1dd2f9bef2d2513702f`（PR #141 合并后）；文档起点为 `0d0cfe82`。2026-10-09 对原结论逐项复核，并在 `docs/overdesign-audit-2026-10-08` 分支实施下述修复。基线证据与本轮处置分别描述；Draft / Proposal 不作为已实现能力。

代码路径省略 `pkg/apiserver/` 前缀；`cmd/`、`deploy/`、`docs/` 相对仓库根目录。历史行号仅适用于原始基线，修复后的源码位置会变化。

## 1. 复核结论与边界

O29–O47 是 **19 个审查条目，不是 19 个均可直接删除的设计缺陷**。确认需要修复的行为问题包括延迟派发清理隔离、写入校验分歧、未实现的更新策略承诺、Job 删除传播策略以及超时单位错误。原文对 stepType、清理标记、迁移、删除事件、权限映射和 installer 的部分判断过宽，已在本文更正。

没有证据支持推翻 API、Domain、Workflow、Infrastructure 分层，也没有证据支持删除 IoC、数据库租约、generation/token fencing、outbox、CAS、UID 校验、授权或 PVC 删除保护。简化应减少当前无用途的结构，而不是改变恢复和安全契约。

本轮未执行真实 MySQL、Redis、Kafka、Kubernetes 或 Harbor 评测验收，也没有负载数据。单元测试、SQLite 和 Kubernetes fake 能验证调用、错误和状态转换，不能证明 MySQL 隔离语义或真实 Kubernetes 垃圾回收。没有调查仓外 Go 消费者。

O01–O21 见历史审计；O22–O28 由草稿 PR #124 使用，相关修复经 #125–#130 合入。本文保留 O29 起的编号以便追溯。

## 2. 处置总表

| 条目 | 复核结论 | 本轮处置及保留边界 |
| --- | --- | --- |
| O29 · P2 | 真实延迟派发缺少清理 fence；五个服务方法没有生产调用 | 已接入真实派发路径并删除死入口；锁、复读、CAS、generation/token 和迁移豁免保留 |
| O30 · P2 | 非法 mode 和重复步骤名确有校验分歧；非法 stepType 已在转换前拒绝 | 共享 mode/重名规则，写入和 Try 均校验；未重写全部错误模型 |
| O31 · P2 | 顶层 strategy 只回显/记录，不控制发布策略 | 空值归一为 rolling，仅接受 rolling；其它值明确拒绝，更新 Current 文档 |
| O32 · P2 | 三处 Job 删除缺少传播策略 | 显式 Foreground 并只固定 UID；Recreate 按 Pod 优雅终止期等待，失败清理不阻塞；完成清理、重试与有意的 Orphan 保持基线 |
| O33 · P3 | 生产固定具备五种存储能力，但有弱事务降级 | 部分收敛：生产启动要求组合能力，SQL/scoped store 编译断言；缺事务/READ COMMITTED 明确失败；窄 DataStore 与断言保留 |
| O34 · P2/P3 | 秒/纳秒缺陷属实；无配置入口字段属实 | 修正换算，移除无入口失败阈值/退避字段与 callback 超时双表示；保留内部空配置保护 |
| O35 · P3 | 单值后端选择器和 noCache 无实际选择作用 | 删除选择器及恒定开关；旧环境变量明确拒绝；MemCache 保留为已有共享测试工具 |
| O36 · P3 | 单 worker 经接口族/切片/断言管理属实 | 收敛为 server 持有一个窄 runtime 接口，移除 errChan；B2 任期容器合并暂缓 |
| O37 · 待设计 | broker 通知与 DB 扫描汇入同一入口 | 保留；每轮仅扫描 100 条，不能把 3 秒轮询周期当成延迟上限 |
| O38 · P3 | 存在重复解析调用点和局部重复校验 | 删除第二次动作冲突校验并复用已解析 replicas；类型化中间模型改造暂缓 |
| O39 · P3 | 四份真正 marker 与一份不同的任务描述混在一起计数 | 共享 marker、模板归一/比较、可重试终态；保留各调用方解码、匹配和失败策略 |
| O40 · 保留 | 启动迁移与运行期保护并非可互相替代 | 保留一次性迁移 marker、滚动升级期间的运行时只读/拒绝保护及 snapshot v1 读取 |
| O41 · P3 | repository 部分写方法和应用服务入口无调用 | 删除死方法与替身样板；事务内包级写函数保留 |
| O42 · P3 | synthetic observe 回调/状态无生产入口 | 删除该状态及无用 option；保留 DeleteFunc、pending generation 和非 adopted 标签占用保护 |
| O43 · P3 | HTTP/gRPC 角色表并存，缺少角色一致性检查 | 新增实际策略一致性测试，共享昂贵请求判定；routeRPC 本身不含角色，未强行推导 |
| O44 · P3 | 无用中间件和不可达选项属实 | 删除未用并发限制器、RateLimit/CORS 冗余选项，集中健康路径判断；健康别名保留 |
| O45 · P3 | 部分校验 tag、错误码、导出副本无用 | 删除未用部分；保留新启用的策略错误码、已有重名错误码和 sync.Once 注册保护 |
| O46 · 保留 | Harbor 直连 Pod 路径是已文档化的共存契约 | 保留双路径及其测试；不把“旧路径”本身认定为可删除代码 |
| O47 · 部分清理 | 一个变量未使用，其余包含安全与自定义清单契约 | 仅删除 REDIS_WORKLOAD_NAME；旧高权限绑定清理、数据库名恢复和 REDIS_WORKLOAD_KIND 保留 |
| B1 · 待设计 | 结果处理仍经 broker 往返，但它是恢复协议的一部分 | 保留；需另行证明背压、公平性、切主、认领和重试行为 |

## 3. 行为修复的证据与契约

### O29｜延迟任务派发的 StatefulSet 清理隔离

基线 `domain/service/workflow/workflow.go` 中 `CreateWorkflowTask`、`ExecWorkflowTask`、`UpdateTask`、`MarkTaskStatus`、`TaskRunning` 没有生产调用。`MarkTaskStatus` 的 Waiting→Queued 分支具有应用锁、应用 ID 复核和清理 fence，但真实 `event/workflow/dispatcher.go` 直接调用 repository 派发 CAS，未经过该检查。

普通延迟任务入队后，后续 StatefulSet 迁移可能失败并留下 pending cleanup；任务到期时必须重新检查。立即任务和 Cron 入队后已经参与活跃任务判断，不能无故增加 locker 依赖。

本轮新增服务派发入口 `ClaimTaskForDispatch` 并接到真实 dispatcher：延迟任务在应用锁内复读 Waiting 状态、generation 和 AppID，非迁移任务检查 pending cleanup，再执行原有 repository CAS。到期的非迁移延迟任务遇到清理未完成时，在同一应用锁内按 Waiting 和 generation 做 CAS 转为 Failed，记录因 StatefulSet 清理未完成而拒绝执行的原因，并沿用现有终态 callback 入口；CAS 未命中视为未认领，不报错。只有应用锁被占用时保持 Waiting，按预期延后记 V(2) 日志并在下一轮派发重试；其他错误仍按失败记录。合法迁移任务保留豁免。删除死方法、孤立 DTO 和旧校验，回归测试通过实际 `claimAndProcessTask` 加真实服务/IoC 注入执行。

**契约影响：** 除 Go 符号精简外，延迟派发行为确有变化，不能写成“仅影响 Go 符号”。HTTP 路由和持久化格式不变。

### O30｜写入与 Try 的 Workflow 校验

基线 `application/application_workflow_steps.go` 的 mode 解析会把未知值当 StepByStep；Try 会拒绝。步骤重名也只有 Try 拒绝。原文称非法 stepType 同样被静默接受不准确：写入在调用转换器之前已经校验 stepType。

本轮共享 `ValidateWorkflowMode`、`ValidateUniqueWorkflowStepName`；保留 Try 聚合错误和写入首个错误两种响应方式。合法空 mode、StepByStep、DAG 的含义不变，步骤名沿用不区分大小写的重名规则。

**契约影响：** 写入不再接受非法 mode 或重复步骤名，详见 `docs/validation-api-guide.md`。

### O31｜顶层更新 strategy 没有执行效果

基线四个枚举 rolling/recreate/canary/blue-green 只进入响应和日志，实际执行由 `traits.rollout` 等组件配置决定。文档承诺 canary/blue-green 会误导调用方；proto 字段存在不意味着这四种策略已实现。

本轮仅接受空值或 rolling，空值归一为 rolling，其余值用现有 `ErrInvalidUpdateStrategy` 拒绝。直接更新、diff-update、dryRun 和无差异提前返回路径都执行校验。

**契约影响：** HTTP/gRPC 校验收紧。rolling 仍是历史占位字段，**不强制资源使用滚动发布**；例如 `traits.rollout` 仍可选择 Recreate。文档改用 Trait 说明真实策略。

### O32｜Job 删除传播策略与等待

基线 `job_instant.go`、`job_scheduled.go` 的失败清理及 `job_batch.go` 的 Recreate 未显式设置传播策略。Kubernetes v1.35 上游 batch/v1 Job 删除策略默认 Orphan，因此可能留下旧 Pod；fake client 不模拟这个行为。

本轮三处显式使用 Foreground，删除只固定观察到的 UID。运行中 Job 的 resourceVersion 会随 status 更新变化，若作为前置条件，读取与删除之间的正常 Pod 进展就会造成 409，因此不携带。Recreate 等待旧 UID 消失后才允许创建，等待上限为 30 秒加 Pod 模板的 `terminationGracePeriodSeconds`（未设置按 Kubernetes 默认 30 秒），避免默认优雅终止期本身耗尽预算。失败清理只发起级联删除，不等待回收，以免多个已创建资源共用的 30 秒清理预算被第一个 Job 耗尽。

取消恢复删除沿用基线：仅固定 UID，等待受调用方 context 约束。完成后日志清理（Background + resourceVersion + 残余 Pod 清理）、重试 attempt 删除及 StatefulSet owner Job 的刻意 Orphan 不变。

**契约影响：** 三处删除改为级联清理，Recreate 会等待；UID 冲突或超时明确失败并保留现场。真实 GC 耗时仍需 Kubernetes 验收。

### O34｜超时单位与无入口配置

基线 `DefaultJobTaskTimeout` 是 `20 * time.Minute`，却在两个“秒数”回退处直接转 int64，得到 1.2×10¹² 秒。小于 1 秒的合法配置会触发其中一条路径。这是 P2 正确性问题，不能仅列作 P3 维护债务。

本轮以 Duration 为配置来源，在进入整数秒模型时向上取整，500ms→1s、1500ms→2s；极大 Duration 限制在可转回 Duration 的完整秒范围，默认回退为 1200 秒。Callback 只保留纳秒上限，删除没有入口的四个 worker 失败阈值/退避字段和不可达退出分支，保留现有退避行为。

13 个普通访问器的 nil/零值保护保留：内部和测试构造不都经过启动 Validate，不能假定每个实例已校验。flags/env 不变，参考 YAML 删除无效字段；见 `docs/workflow-architecture-guide.md`。

## 4. 存储、模型与领域服务

### O33｜必需存储能力与测试边界

基线五种能力为 ConditionalCompareAndSwap、DatabaseClock、Transactional、ReadCommittedTransactional、RowLocker。生产 SQL Driver 和授权 scoped store 均实现它们；静态检索有 27 个文件、63 处断言。

“将全部能力并入最底层 DataStore”不是无成本简化：本轮编译评估影响 13 个测试包、至少 25 种替身，其中包含只读消费者。强行补空事务/假锁会削弱测试可信度。

本轮在生产初始化要求组合契约 `RuntimeStore`，SQL Driver 和 account.Store 有编译期断言；应用创建、Workflow 取消不再降级为非事务执行，两个租约路径不再把 READ COMMITTED 降级为默认隔离。缺少能力必须在业务回调运行前明确失败。保留窄基础接口、各消费者能力检查与真实事务内连接契约；未声称消除了全部断言。

### O38｜组件变更的重复解析

基线是 11 个文件中有 **17 个静态解析调用点**，不是每个请求必定解析 17 次。阶段职责及数据表示并未逐一证明等价，不能仅凭调用数就增加统一中间模型。

本轮删除对同一请求子集的第二次动作冲突检查，复用组件转换已解析的 replicas。保留原错误顺序、adopted/shared 限制、snapshot 验证和无变化时不落库。更大解析流水线改造暂缓。

### O39｜清理 marker 的共享边界

基线有四份真正的清理 marker，另有一份任务清理组件描述；后者不能视为相同实体。原文所称“后两处 writer 的 omitempty 不同”也不成立，那两处是读取结构。

本轮在已有 `domain/model/workflow_queue.go` 共享 marker、六种可重试终态及模板名去空白/去重/排序/比较。持久化 writer 的 omitempty、v2 marker 的 version 0、v3 marker 的 version 3 保持不变，并有字节格式回归。

解码失败、跳过无关 Job、重复 Job 拒绝、identity 匹配、PVC 删除保护仍由各调用方决定；没有把不同的失败策略合成一个宽泛解析器。

### O40｜保留迁移与运行期保护

`mysql/migrations.go` 的 marker 使迁移一次性完成，并非每次启动重新消除旧数据。滚动升级期间旧 writer 仍可能在迁移后写入旧值，因此 runtime 对空 management_mode 的历史推断、对未知非空模式的 observe 保护不能仅因存在迁移就删除。

资源预算 interval 回退同样需要考虑旧 writer 和存量记录。snapshot v1 目前没有对应的启动转换，不能归类为“迁移和回退重复执行”。本轮保留这些路径，不改变旧库支持范围，不删除旧表/列。

### O41｜删除死接口，保留事务写入

基线 Application/Component/Workflow repository 有多种未被生产使用的写方法，应用服务的 GetApplication/DeleteApplication 也没有生产消费者。真实删除走 DeleteApplicationCascade；它包含锁、任务取消和缓存清理契约。

本轮移除无调用接口方法及实现/替身样板，删除死服务入口，相关行为测试改走真实级联路径。包级 repository 写函数是事务中使用指定 store 的实际入口，予以保留，不再称为单纯的 backward compatibility API。应用包七个纯转发函数改为直接调用 workflow 服务包。

## 5. 装配、接口与部署

### O35、O36｜移除没有实际选择的结构

生产后端固定 MySQL/Redis，本轮删除 datastore-type/cache-type 选择器以及恒为 false 的 noCache。`ERUUN_DATASTORE_TYPE`、`ERUUN_CACHE_TYPE` 即使为空也会明确报错，旧 flags 由参数解析器拒绝；部署清单、Helm 和使用文档已移除它们。MemCache 作为多包共用的现有测试工具保留，避免新建仅为搬代码的包。

单 Workflow worker 收敛为 server 持有一个具有生命周期与 RuntimeStats 方法的窄接口，保留测试替换和现有 IoC 注入，删除单元素切片、可选能力断言及未读 errChan。Controller/Scheduler 两个任期容器（B2）保留；合并必须另行验证启动失败、就绪屏障、Informer 切换和释放 Lease 前退出顺序。

### O42｜保留真实删除事件与标签保护

未配置 WithPodObservationFunc 的生产 coordinator 不需要 observedPods、observeMu 和 synthetic observe 回调，相关状态及无用 option 已删除。

原文把 `handleDeletedPod` 的零覆盖率当成无入口证据不成立：DeleteFunc 已注册，会递增 pending generation，防止旧工作覆盖后续事件。非 adopted binding 也用于阻止错误标签声明，不能因为不进入 adopted 观察集合而删除。两类行为均保留并补回归。

### O43、O44｜接口策略与中间件

routeRPC 只有 HTTP→RPC 映射，**没有角色信息**，无法独自推导两侧授权。复核未发现已有角色漂移。本轮把 HTTP 实际策略读取与授权执行共用，增加与 gRPC 实际角色表逐项比较的测试；昂贵请求分类共用同一函数。

删除没有生产调用的全局并发限制中间件，RateLimit 只使用共享 limiter，CORS 只保留现有生产使用的精确 origin 与 credentials 路径。健康路径判断共用；`/api/v1/health`、`/api/v1/ready` 是公开路由，即便文档未列出，也不因此删除。未知业务方法拒绝、Bearer/空间授权和强制改密门禁不变。

### O45｜未使用 tag、错误码与映射副本

删除四个未用 tag 及其辅助正则、无用 RouteRPCMappings 副本和未使用错误码。原候选集合中 ErrDuplicateComponentName 已有调用，ErrInvalidUpdateStrategy 在 O31 启用，两者保留；文档仍应列出可能返回的策略错误。

`init()` 与 InitValidator 虽都触发初始化，但 sync.Once 已防止重复注册；不是双注册缺陷，现有错误报告及 checkname、错误码重号检测保留。

### O46｜Harbor 双路径是 Current 契约

Current 文档与 Runner README 明确支持 sandboxURL 与直连 Pod 路径共存；仓库也存在 Sandbox 专项测试，不能仅凭默认 fixture 不带 sandboxURL 认定新路径未测试。本轮不删除旧路径，也不撤销共存承诺。

### O47｜installer 只能删除确认未使用的变量

本轮仅删除 REDIS_WORKLOAD_NAME。旧 cluster-admin binding 清理是收紧遗留权限的安全动作；旧 Secret 缺少 database 字段时从 StatefulSet 恢复数据库名，是防止升级误连默认库的保护；REDIS_WORKLOAD_KIND 配合自定义 MANIFEST 使用，不能只看内置 StatefulSet 就删除 deployment 支持。相关原建议撤回。

## 6. 消息与恢复协议：暂缓设计变更

**O37：** 延迟通知先持久化检查点，再通过 broker 降低发现延迟；数据库每 3 秒最多扫描 100 条记录。周期不等于在积压、失败或限流下的最大延迟。删除 topic、消费组和健康项前需要明确容量与延迟目标，并验证准入、授权、先建 outbox 后标记 dispatched 等恢复约束。

**B1：** 同一 Leader 内 outbox 发布、broker 读取/认领、DB 结果认领确有往返。但处理 token 是新生成 UUID，并非简单复用 broker message ID。改为数据库直接驱动的有界处理池，需要验证扫描公平性、索引成本、背压、长任务占槽、切主接管及失败重试；本轮不改变协议。

## 7. 邻近低优先级清理

- artifacts 的 12 个可选 executionKey 签名改为显式字符串，空 key 仍保留原 ID 公式与查询语义；PutResult 便利入口移入测试文件。固定 ID 回归覆盖空 key 与非空执行身份。
- 删除无消费者的 RestServer 命名 bean、只写 KubeConfig/runtimeQueues 字段；保留 runtimeReadiness bean，因为它仍承担实际 IoC 注入。
- Informer 的无任期测试入口、ResetPodSnapshots 和 WithSyncTimeout 移入测试文件；生产继续使用 generation fencing 入口，删除无消费者 GetStats。
- CloudProvider 去掉无消费者 SupportedActions，测试直接验证允许的 action；删除 job 包的注册转发，保留实际注册表和源码接入模板并同步文档。
- 删除无读写的 SystemInfo 模型注册与类型，停止为新库创建该表；没有执行或新增任何 DROP TABLE。JobInfo.Production/TargetEnv 同时属于 HTTP/gRPC 输出契约，本轮保留。

## 8. 验证与剩余边界

本轮按真实入口增加或调整回归：派发 fence、步骤校验、strategy 提前返回、删除策略/冲突/等待、亚秒和极大超时、缺少事务能力、marker 字节格式、删除事件代次、标签占用、角色一致性、旧配置拒绝以及 artifact ID 保持。

2026-10-09 最终验证通过：

- `go test ./... -race -cover`：53 个包报告成功，没有失败或数据竞争报告。
- `go vet ./...`、服务端 `go build -trimpath`。
- `go test -tags=integration ./... -run '^$'`：仅确认集成测试代码可编译，没有执行数据库验收。
- installer 回归、`go test ./deploy`、Helm v4.2.0 lint 和 template 检查。
- 本地 Docker 镜像构建，未推送镜像。
- 修改文件 gofmt、`git diff --check`、敏感内容检查。
- 对事务/scoped store、正常 Workflow 输入和 HTTP/gRPC 权限的独立定向 diff 复核，未发现新增回归。

全仓首轮发现一个 API 测试包装器漏传 READ COMMITTED 能力，补齐后失败用例、API 全包和最终全仓 race 均通过。实现过程中 `TestInstantJobRetryCheckpointResumesWithoutDoubleGrowth` 偶发失败，基线 `0d0cfe8` 同样可复现（200 次中 2 次）。原因是 `database/sql` 在 context 结束后异步回滚事务，随后的 Commit 可能只返回 `sql.ErrTxDone`，丢失取消原因，使正常停止被归为基础设施错误。SQL Driver 的两种事务入口现在在 context 已结束时把 `ctx.Err()` 与 `ErrTxDone` 一并返回。

真实 MySQL 隔离/锁、Kubernetes GC、Harbor 评测和负载验收仍未执行；不会用本地测试代替这些结论。

本轮未提交或推送。后续设计项为 B1/B2/O37、O38 更大解析流水线；O40/O46/O47 的现有兼容或安全契约继续保留。没有为了减少代码而删除当前仍承担正确性职责的路径。
