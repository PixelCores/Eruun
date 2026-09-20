# 第一阶段：万级长期 Harbor Job 稳定运行计划

> 状态：Draft / Proposal。本文是第一阶段主 PR 的实施与验收计划，不代表万级容量、按需 Sandbox 或周级运行已实现。代码核查基线为 `8290fb1`；所有吞吐算式均为静态估算，真实容量待验证。

> 后续细化见 [2026-09-20 方案讨论纪要](harbor-runtime-stage1-discussion.md)：补充虚拟节点部署、可变 task/trial 并发、真实创建预算和采集边界。本文第 4 节及 P1-04 最初按 Harbor 创建 Sandbox 描述；最新推荐路线为 Eruun 统一创建、Harbor 逐 trial 申请并使用环境，尚待冻结协议及故障/保留策略，不能把初稿分工视为最终决定。

## 1. 已确认目标与交付边界

Eruun 部署在 ACK 内并管理同一集群，分批创建任务，目标是至少 **10,000 个 Harbor eval Job 同时实际执行**。单个任务常见运行时间为 1–2 小时，复杂任务可能持续数周。优先保证稳定性，不要求低启动延迟，不引入预热池作为前提。

目标执行结构为 Kubernetes Runner Job 加按需创建的 `agents.kruise.io` Sandbox；Sandbox 的任务环境使用支持后续 Checkpoint 的 ACS Agent Sandbox 算力。ACK 控制面与 ACS 执行算力分别核验，普通节点上的 Pod 不自动获得 Checkpoint 能力。现有 HTTP/gRPC、空间授权、WorkflowQueue、JobInfo、数据库执行租约和结果完整性门禁继续作为基础。

第一阶段交付创建、观察、执行、取消、结果收集及控制面故障接管。第二阶段交付文件系统 Checkpoint 和新进程从已保存任务进度恢复；Runner 或任务环境本身丢失时，第一阶段必须给出明确失败/异常证据，不宣称能恢复尚未保存的计算进度。

本阶段不包含多集群动态连接、SandboxSet/SandboxClaim 预热池、进程内存恢复、新的通用工作流框架或生产环境直接压测。创建速率、并发上限和实际容量分别管理，配置接受 10,000 不等于容量已达标。

## 2. 主 PR 与计划维护方式

- 本主 PR 的目标分支为 `main`，初始提交只包含本文和文档索引；保持 Draft。
- 下文 P1 工作包是后续子 PR 的拆分计划，本轮不创建额外子 PR 或 Issue。子 PR 合入本阶段主 PR 的集成分支；具体验收证据、子 PR 链接和完成状态统一维护在主 PR 描述中。
- 第二阶段主 PR 同样以 `main` 为目标，其实施依赖本阶段身份、Sandbox、长期运行和状态观察契约稳定；主 PR 描述互相链接。
- 每个工作包完成后更新事实、风险与验证结果。阶段完成后才将已实现内容迁入 Current 文档，并在主 PR 中给出最终容量报告；计划与压测结果不能互相替代。

## 3. 当前事实与容量风险

| 当前事实 | 万级场景的影响 | 代码依据 |
| --- | --- | --- |
| 全局默认准入 100、单空间 10；Worker 默认每进程 100 个 Workflow controller | 默认部署无法形成 10,000 并发；增加副本还增加缓存、连接和事件复制 | [调度策略](../pkg/apiserver/workflow/config/job_scheduler.go)、[运行配置](../pkg/apiserver/workflow/config/runtime.go) |
| 运行 Job 每 2 秒读父任务 ownership 并 GET Kubernetes Job | 10,000 运行任务理想节奏约为 5,000 DB GET/s 和 5,000 Kubernetes GET/s | [Job 等待](../pkg/apiserver/event/workflow/job/job_retry.go)、[轮询周期](../pkg/apiserver/event/workflow/job/job_batch.go) |
| Runner 每 15 秒心跳，事件鉴权读取 Pod 和 Job | 约 667 事件请求/s，另约 1,333 Kubernetes GET/s，还存在 DB 鉴权与事务成本 | [Runner](../runners/harbor/runner.py)、[事件鉴权](../pkg/apiserver/jobs/service.go) |
| 每 Workflow 默认 10 秒续租；取消 watcher 每秒 GET Redis | 约 1,000 次续租/s 和 10,000 Redis GET/s；排队中已由 Worker 持有的任务还每 200ms 查询准入 | [租约](../pkg/apiserver/event/workflow/workflow.go)、[取消](../pkg/apiserver/workflow/signal/cancel.go)、[准入等待](../pkg/apiserver/event/workflow/job/job_scheduling.go) |
| Scheduler 默认每 3 秒运行，单批最多准入 100，扫描全部 queued/admitted 并逐 task 查 parent | 长事务、全局策略行竞争与重复读取；100/3 仅为默认批次节奏估算 | [调度仓储](../pkg/apiserver/domain/repository/job_scheduler.go)、[Dispatcher](../pkg/apiserver/event/workflow/dispatcher.go) |
| 结果维护循环每 15 秒处理最多 20 个正常 pending 保存目标，串行执行 | 单目标结果正常处理节奏约 1.33 个/s 或更低，集中完成可积压；恢复过期目标另计 | [维护循环](../pkg/apiserver/jobs/service.go)、[保存](../pkg/apiserver/jobs/artifacts/delivery.go) |
| eval 默认 1 小时、最多 24 小时，Runner 与 trial 也有 deadline | 周级任务当前不支持，不能只改一个 API 校验值 | [规格](../pkg/apiserver/domain/spec/job.go)、[构建](../pkg/apiserver/jobs/builder.go)、[Runner](../runners/harbor/runner.py) |
| Runner 显式关闭 SandboxClaim；任务环境沿用 Harbor ACK Pod 后端 | 尚无按需 Sandbox CR 执行与生命周期适配 | [环境适配](../runners/harbor/eruun_environment.py)、[Runner 配置](../runners/harbor/runner.py) |

现有 Worker Pod Informer 只筛选带 `eruun.io/app-id` 的 Pod；独立 eval Runner/trial 使用 task 标签，不能把应用组件的共享缓存视为独立 Job 已完成共享观察。应用组件路径还有每次事件遍历全部 Pod 的 tracker，以及每个等待器查询全部缓存的 lister；它们是复用前需修正/测量的相邻边界，不可误归因于纯独立 eval 压力。[观察器](../pkg/apiserver/infrastructure/informer/kubernetes_observer.go)、[tracker](../pkg/apiserver/infrastructure/informer/waiter.go)。

## 4. 数据、身份与职责契约

下表描述现有身份及拟新增关联的所有权，不定义新的公共字段、路由或表。P1-01 必须决定最小存储改动；可复用现有结构时不增设平行实体。

| 信息 | 权威来源 / Writer | Reader 与使用位置 | 持久化和身份要求 |
| --- | --- | --- | --- |
| 空间、namespace、访问权限 | Account/Workspace 领域与授权路径 | 提交、查询、Runner、资源清理 | 沿用 workspace ID 和 namespace 绑定，跨空间访问始终拒绝 |
| 任务生命周期和执行归属 | Scheduler/Worker 经数据库 CAS 写入 WorkflowQueue | 调度、接管、取消、终态写入 | 沿用 taskID、runGeneration、runToken、workerID；cache 不替代 ownership |
| Job 执行与采集事实 | 当前执行写 JobInfo，Runner 经鉴权上报事件 | API 查询、结果保存、故障诊断 | 沿用 executionKey、attempt、事件顺序和结果完整性校验 |
| Runner Kubernetes Job/Pod 身份 | Kubernetes 返回对象，Eruun 校验并关联 | 观察、Runner 鉴权、精确清理 | namespace/name 加 UID；同名重建不能继承旧执行身份 |
| trial 对应 Sandbox/Pod 身份 | Harbor 环境适配负责创建，Sandbox controller 写实际状态 | Runner 执行、状态观察、第二阶段恢复 | 关联 task/execution/trial 与 Sandbox、Pod UID；具体存储形状在 P1-01 冻结 |
| Sandbox 基础设施状态 | 安装版本对应的 Sandbox controller | Eruun 只读观察及故障归因 | 保留 resourceVersion、conditions、删除状态与观察新鲜度；Running 不等于评测成功 |
| 最终结果与保存目标状态 | Runner 完整上传，现有 artifacts 模块持久化与交付 | 用户查询、下载、保留策略 | 任务成功、采集完整、各目标保存成功分别记录，不互相替代 |

实例健康与任务成功分别判断。Informer 提供最终一致快照，不作为无遗漏的事件审计；初始未同步、Watch 中断及重建期间不得把“不在缓存”解释为成功或授权通过。

## 5. 实施工作包与依赖

### P1-01：基线、契约与验收预算

- 工作：复用现有压测目录增加完整完成观察能力；记录代码/镜像版本、每任务 RPC/SQL、调度轮耗时、运行槽位、缓存/队列长度和保存滞后。冻结 Job、trial、Sandbox、Runner 的计数口径、身份关联和最大执行时长。
- 触点：`examples/agent-evaluation/load-test`、现有 jobs/workflow 可观测性、本文及 Current 契约文档的后续变更清单。
- 验收：1、10、100 个真实 Harbor Job 端到端正确；所有运行与完成计数可追溯到任务身份；冻结下文环境和 SLO 表。固定版本 Harbor 及 ACS 控制器兼容性由测试证明。
- 依赖与退出：无。若 ACS 环境或指标缺失，记录具体阻塞与提供者；可继续纯代码优化，但不得填写真实集群结果。

### P1-02：共享资源观察与有界状态协调

- 工作：为独立 Job/Runner Pod 增加准确的归属选择器；原生 Job/Pod 使用 typed informer，Sandbox 使用限定 GVR 的 dynamic informer，复用集群内连接。按 namespace/name、UID 和任务归属建立必要索引；事件只触发对应任务的有界协调，合并重复事件。
- Worker 完成判断继续独立于 Controller Leader；Controller 负责状态投影。每个进程按资源类型共享观察，测量 Worker 扩容导致的 Watch/缓存复制，达到瓶颈才决定 namespace/任务分片，不先建新事件总线。
- 触点：`infrastructure/informer`、`server_assembly.go`、Job 等待路径、`jobs/service.go` 中 Runner 鉴权、Helm/stack/workspace RBAC。
- 验收：重复/乱序通知、删除重建 UID、初始同步失败、断线及过期 resourceVersion 重建、跨空间访问全部覆盖；普通终态观察不再每任务每 2 秒远程 GET。授权与破坏性操作仍有明确的新鲜度、失败关闭和 ownership 校验，不能机械地把鉴权 GET 全换成 cache。
- 依赖与回退：依赖 P1-01；回退前先降低准入并排空不兼容资源，不设置静默轮询降级掩盖故障。

### P1-03：调度、执行租约和取消的规模化

- 工作：去除每轮全部活动 Job 的 parent N+1 读取和 Worker 200ms 准入忙等；按候选批量读取、明确事务边界和索引。沿用优先级/FIFO/等待老化、空间公平和 DB CAS，批量化或事件唤醒只作为可测量的实现选择。
- 测量长时间占用 Workflow controller 的内存与 goroutine；先削减外部重复轮询，再决定是否将等待改成按任务键协调。续租/取消可以合并批次，但必须逐任务保持 fencing，不能因为优化而接受已失效 owner。
- 触点：workflow dispatcher/controller、`domain/repository/job_scheduler.go`、workflow lease、`workflow/signal`、数据层索引。
- 验收：目标负载下调度周期、锁等待、续租延迟符合预算；满并发、公平性、限额降低、用户取消、过期 lease 和旧 Worker 写入均正确；MySQL 集成验证真实锁与事务，不用 SQLite 替代。
- 依赖与回退：依赖 P1-01，可与 P1-02 并行；默认策略及 wire 契约的任何改动须同步文档。回退需先排空或兼容读取新增状态。

### P1-04：按需 Sandbox 与长期执行

- 工作：在现有 Harbor 环境适配中按需创建/关联 Sandbox，覆盖执行命令、文件传输、就绪/失败、取消和清理；固定可用 ACS/CRD/Harbor 版本，保留原有非 root、权限隔离和制品完整性边界。
- 将试验实际资源身份记录与 Runner 关联持久化；处理创建响应丢失、重复交付、同名对象替换和 OwnerReference/删除传播，确保控制面重启不会误删仍健康的环境。
- 协同修改 Go/Python 校验、Runner/任务环境 deadline、Harbor task 内部超时、凭据有效期与结果保留；最大任务时长必须显式有界。已有资料未证明的 ACS 单实例时长/配额要在集群验证；不能仅去掉 24 小时校验。
- 触点：`runners/harbor`、`jobs/builder.go`、domain/spec、HTTP/gRPC 校验/Schema、空间 RBAC、部署文档与示例。
- 验收：真实 ACS 按需创建到完整结果保存；无需预热池；控制面重启后重新关联健康实例；Runner/任务环境丢失有明确结果。现有独立与 Application eval 契约均回归。第二阶段只预留可靠关联，不提前实现快照 API。
- 依赖与回退：依赖 P1-01、P1-02。资源类型切换不能把存量 Pod 静默当作 Sandbox；停止新准入并按原契约排空存量后回退。具体暴露/启用方式由实现 PR 明确，本文不承诺新配置键。

### P1-05：启动限速、背压与结果保存

- 工作：服务端约束实际资源创建，分别管理运行容量、启动中容量、持续创建速率和突发额度；多个副本共享同一预算，重试、接管重建与 Runner 内多个 trial 的创建都计入，单纯限流 POST /jobs 不够。
- 根据 Pending、镜像拉取错误/延迟、ACS 配额、API 429 和保存积压实施可解释背压；使用有界退避与抖动，取消应解除等待，重启不能丢失已准入/已启动事实。
- 扩展现有结果交付循环的有界吞吐、分页与 lease 协调，保留大制品流式处理、完整上传门禁、幂等保存及失败可重试语义；避免把大对象堆积在内存或全量数据库读取中。
- 触点：调度策略和资源创建路径、Harbor 环境适配、artifacts delivery/保留、配置与监控。
- 验收：突发提交及副本扩容均不越过冻结预算；多 trial 与恢复重建实测包含在预算内；集中完成后保存积压可排空；一个慢目标不拖死全部任务。
- 依赖与回退：依赖 P1-03、P1-04；先冻结同一空间/全局配额职责。可降低速率与并发进行回退，但不能丢弃已接受任务或伪造已保存结果。

### P1-06：容量、长稳、故障与交付验收

- 工作：执行下文矩阵，定位首个瓶颈、修复后仅复测受影响场景及最高通过档；将结果和配置快照附在主 PR。
- 验收：满足全部阶段完成条件后更新 Current 文档、HTTP/gRPC 示例、安装/运维说明；版本号只在明确发布变更时按仓库规则共同更新。
- 依赖与回退：依赖 P1-02 至 P1-05。未通过的档位停止注入，按任务身份排空/取消并保存证据，报告实际通过上限。

建议顺序：P1-01 → P1-02 与 P1-03 并行 → P1-04 → P1-05 → P1-06。发现契约互相影响时在主 PR 更新依赖，不另建平行调度器或任务状态源。

## 6. 压测口径、资源预算与验收

### 6.1 “10,000 同时执行”的定义

主指标是同一观测窗口内有 10,000 个不同逻辑 eval Job，其当前 Runner 有效、对应 Kubernetes Job 活跃，并且至少一个对应 trial Sandbox/Pod 在实际执行；不以 HTTP 202、DB queued/running 单字段、已创建但 Pending 的 Job 数或历史累计数代替。固定每 Job 的任务数量、attempts 和 trial concurrency，并单独记录阶段切换时暂时没有运行 trial 的 Job 数。

在 concurrency=1、每 Job 一个活跃 trial 的情形，约需 10,000 Runner 加 10,000 trial 执行实例，另计系统资源、重试/回收重叠。按当前每侧默认 1 CPU/2 GiB request 粗算为约 20,000 CPU/40,000 GiB，属于资源请求估算而非必须固定的压测规格。降低合成负载资源时必须记录，不能替代真实负载验证。[资源默认值](../pkg/apiserver/domain/spec/job.go)。

ACR 企业版标准版的官方分发规格为 500 拉取 QPS；此为实例规格，不是 500 Job/s。必须记录实际 ACR 规格、镜像层/大小、缓存命中、带宽与拉取延迟，再确定创建预算。稳态吞吐还受模型 Provider 配额、ACS 算力/网络配额、MySQL、Redis/Kafka 和结果存储制约。

### 6.2 执行前冻结的验收表

下列建议值用于讨论和实验设计，不是产品 SLA；P1-01 在首轮正式压测前填写并固定，变更必须另开实验轮次，不能在失败后追改阈值。

| 项目 | 冻结内容 / 建议起点 | 责任与完成时点 |
| --- | --- | --- |
| 最大任务时长 | 明确覆盖目标周数的有界时长、单 trial 时限、采集余量与凭据周期 | 产品与运行时维护者，P1-01 |
| 10,000 稳态 | 建议每轮维持至少 2 小时，最高通过档独立重复两轮 | 压测负责人，P1-01 |
| 状态收敛/取消/接管 | 建议 p99 状态收敛 ≤15s、取消确认 ≤30s；控制面失效后的重新接管目标 ≤60s，实际资源终止另测 | 运行时维护者，P1-01 |
| 正确性 | 已接受任务全部可核对；不允许无法解释的丢失/重复副作用、旧身份写入或误报成功 | 各工作包共同保持 |
| 长稳 | 建议先 72 小时稳定子集，再覆盖声明最大时长的代表性真实任务；伪时钟测试不替代长稳 | 运行时与环境负责人，P1-06 |
| 结果排空 | 按平均/最大制品、保存目标数冻结最大积压量及排空时限；任务终态与全部目标成功分开计时 | 结果模块维护者，P1-01 |
| 资源及停止线 | 集群/数据库/镜像服务配额、预算、连接/内存上限及 429/5xx/续租延迟停止阈值 | 环境负责人，注入前 |

### 6.3 实验矩阵

| 实验 | 规模与变化 | 必须回答的问题 |
| --- | --- | --- |
| 功能基线 | 1、10、100 个真实 Harbor oracle 任务 | Sandbox、身份、命令/文件、结果、取消是否正确 |
| 并发阶梯 | 100 → 1,000 → 3,000 → 10,000，逐档排空、固定其他条件 | 哪一层首先饱和，活跃 Job/Runner/trial 是否达到目标 |
| 创建速率 | 固定并发预算，分段提升速率；冷镜像和缓存命中分别测 | 服务端背压、多 trial 创建及 ACR 限制是否真实受控 |
| 稳态与完成波峰 | 长任务稳定驻留；分散完成和集中结束分别测 | 心跳、DB、Watch、内存及结果保存能否稳定、不持续积压 |
| 故障 | Worker/API/Controller/Scheduler 分别滚动重启、Leader 切换、Watch 断线/重建、DB/队列短暂中断 | 健康执行能否接管，失效身份是否被隔离，取消是否收敛 |
| 资源失败 | Runner/trial 丢失、ACS 配额不足、拉取失败、上传响应丢失、保存目标变慢 | 阶段一的失败结果是否可信，是否越权/重复创建/错误清理 |
| 长时间与保留 | 跨日/周级 deadline、凭据轮换、结果保留/清理与慢采集 | 超时语义一致，无租约泄漏、资源泄漏或提前删除 |

复用 [现有压测计划](harbor-job-load-test-plan.md) 与 [提交器](../examples/agent-evaluation/load-test/submit.py)，增加服务端阶段时间与受控状态采样。现有 60–300 秒休眠任务只适合预检，必须扩展时长以覆盖爬坡和稳态窗口；提交器自身落后时该轮目标 QPS 不成立。独立提交当前没有客户端幂等键，响应不确定时先按运行记录核对，不能盲目重试创建。

报告包含：接受率与实际提交速率、实际运行 Job/Runner/trial 数、排队/启动/状态/取消/接管 p50/p95/p99、运行时 CPU/RSS/goroutine/缓存与协调积压、Kubernetes QPS/429/重连同步时间、MySQL 查询/锁/续租延迟、队列与 Redis 压力、ACR 拉取及节点 Pending 原因、制品字节数/保存吞吐/排空时间。所有指标记录分子分母和采样误差，不记录 Token、私有任务内容或连接密钥。

## 7. 验证与完成清单

每个实现 PR 使用触及包的 Go/Python 行为测试；共享状态变更运行 race，MySQL 事务变更运行已配置隔离 MySQL 集成测试，接口变更同步 HTTP/gRPC/Schema/示例，RBAC/部署变更执行仓库安装器与 Helm 检查。假客户端可验证乱序/身份逻辑，但不能建立真实 Watch、ACS 或容量结论。遵循仓库 CI，不以计划提交代替实现验证。

- [ ] P1-01 基线、资源环境、最大时长与 SLO 已冻结。
- [ ] P1-02 共享观察与身份/新鲜度边界通过。
- [ ] P1-03 调度、续租、取消在目标规模和故障下正确。
- [ ] P1-04 按需 Sandbox 与长期执行链路完成。
- [ ] P1-05 创建预算和结果背压在多副本下成立。
- [ ] P1-06 两轮万级稳态、故障与长稳证据齐全，残余限制公开。
- [ ] Current 文档、例子和阶段二依赖契约同步；主 PR 才具备转 Ready 条件。

## 8. 官方依据

- [阿里云：创建 Agent Sandbox](https://help.aliyun.com/zh/cs/user-guide/create-an-agent-sandbox)：组件、API 版本、实例与预热池关系。
- [阿里云：Checkpoint 克隆](https://help.aliyun.com/zh/cs/user-guide/clone-agent-sandbox-using-checkpoint)：按需 Sandbox CR 示例、ACS 算力要求与第二阶段文件系统恢复限制。
- [阿里云：ACR 规格](https://help.aliyun.com/zh/acr/product-overview/billing-description)：不同规格的拉取 QPS，不能直接换算为 Job/s。
- [Kubernetes API 概念](https://kubernetes.io/docs/reference/using-api/api-concepts/)：List/Watch 与 resourceVersion 恢复语义；部署时核对实际服务端版本。
- [client-go v0.35.0 SharedInformer](https://github.com/kubernetes/client-go/blob/v0.35.0/tools/cache/shared_informer.go)：共享缓存、最终一致和 resync 语义；resync 不等于每轮向服务端全量 LIST。
