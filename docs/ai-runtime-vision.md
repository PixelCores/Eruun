# Eruun AI Runtime 愿景与演进边界

> 状态：Draft / Proposal。本文说明 Eruun 的产品方向、能力分层和实施门禁，不代表 `main` 已经提供 Agent、MCP、评测、向量化、模型服务或通用 AI 云平台 API。

> 示例说明：本文图示仅用于表达概念边界，不是可执行部署清单或公共契约。

## 1. 为什么需要这份总纲

Eruun 已经是一套可运行的 Kubernetes 应用与工作流运行时，但“能够调度容器”并不等于“已经是 Agent Runtime”。Agent 会长期或按任务运行，调用本地 CLI、远程 MCP Server 和模型服务，处理用户数据与凭据，并产生需要审计的外部副作用。模型服务和评测又引入 GPU、制品、配额、成本和可重复性问题。

这份文档用于约束演进方向：先复用现有 Application、Component、Traits、Workflow 和 Job，只有证据表明现有边界无法表达需求时，才引入新的公共实体。复用 Workflow/Job 执行链路不要求每次独立任务都先创建 Application 或 Component。专题 Proposal 可以探索实现，但不得越过本文把探索字段描述成稳定契约。

## 2. 当前事实与目标能力

| 能力面 | `main` 当前事实 | 目标方向 |
| --- | --- | --- |
| 控制面 | API/controller/scheduler/worker 四角色；MySQL 保存 Workflow 状态和执行 ownership | 继续作为 Agent、评测与模型任务的统一控制面 |
| 工作负载 | `webservice`、`store`、`job`、`scheduledjob`、`cloudjob`、`config` 和 `secret` 组件；Service 由 Trait 声明 | 表达常驻 Agent、一次性 Agent 任务和模型服务，但暂不冻结新组件类型 |
| 扩展能力 | 已实现 storage、env、resources、securityPolicy、RBAC、probes、init、sidecar、ingress、service、share、rollout 等 Traits | 增加 Agent 所需能力前先判断能否组合已有 Trait，避免按产品名新增专用 Trait |
| 安全 | 账号与空间授权、Kubernetes RBAC、容器 SecurityContext、Secret/ConfigMap 引用和 URL 安全策略 | 增加工具授权、出站访问、凭据委派、审批、审计和撤销的一致边界 |
| 执行 | Workflow 支持 StepByStep/DAG、审批、取消、超时、回调、租约和 fencing | 承载 Agent 任务、评测、数据处理及模型制品准备 |
| AI 专用能力 | 尚无 Agent、MCP、评测、向量化、vLLM 或通用托管模型 API | 按本文路线图逐层设计、实现和标记 Current |

## 3. 六类目标能力

### 3.1 Agent 执行

Eruun 应能承载三种执行形态：提供网络端点的常驻 Agent、由事件或 API 触发的一次性 Agent 任务，以及按计划运行的 Agent 任务。每种形态都需要明确镜像、命令、输入输出、生命周期、资源、终止和重试语义。

近期不创建独立顶层 Agent 数据库实体。设计应先证明现有 Application/Component/Workflow 无法表达某项稳定需求，再决定是否增加公共类型。

### 3.2 MCP 与 CLI 工具

工具声明至少要回答：工具在哪里运行、如何发现、允许调用哪些能力、输入输出如何校验、凭据由谁持有、调用是否需要审批，以及产生哪些审计证据。

MCP Server 和 CLI 是两种运行绑定，不是权限本身：

- 远程 MCP 可能使用 HTTP 传输和独立授权服务器；本地 MCP 或 CLI 可能作为主容器、sidecar、init container 或受控子进程运行。
- 工具名称、描述和注解都属于不可信输入，不能据此自动授予文件、网络、Kubernetes 或云权限。
- 运行时不得把访问 Eruun 的登录 Token 直接传给 MCP Server 或下游 API；下游访问应使用面向目标资源的独立凭据。
- 高风险、副作用或越出既定能力边界的调用需要可配置的人类审批，而不是依赖模型自行判断。

具体公共字段、Trait 名称和传输支持矩阵要在实现 PR 中根据测试场景确定，本文不预先冻结。

### 3.3 权限、凭据与隔离

Agent 权限必须分层描述，不能用一个笼统的“permissions”开关覆盖所有边界：

| 层次 | 需要控制的内容 | 可复用基础 |
| --- | --- | --- |
| 平台授权 | 谁可以创建、执行、观察、取消或审批工作负载 | Eruun 账号、空间和角色授权 |
| Kubernetes 身份 | Pod 可访问哪些 Kubernetes API 和资源 | ServiceAccount、RBAC Trait、namespace 隔离 |
| 容器权限 | 用户、Linux capabilities、只读文件系统、权限提升 | SecurityPolicy Trait 与 Pod Security |
| 数据与凭据 | 哪个执行可以读取哪个 Secret、ConfigMap、数据集或制品 | 引用式挂载、短期凭据、按任务作用域授权 |
| 网络访问 | 允许访问的 MCP、模型、对象存储和外部 API | NetworkPolicy、URL 策略、出口代理或策略执行点 |
| 工具调用 | 可发现和调用的工具、参数范围、副作用级别、审批要求 | 待设计的能力策略与审计事件 |

默认策略应为拒绝未声明能力、最小权限、凭据不进入普通日志或任务载荷、任务终止后可撤销。集群级权限、宿主机挂载、特权容器和任意命令执行不能由一个普通 Agent 声明自动获得。

### 3.4 模型服务与 GPU

Eruun 的目标是同时支持外部模型端点和 Kubernetes 内自托管模型。自托管模型需要处理模型制品准备、不可变 revision、GPU/显存资源、单节点或多节点拓扑、健康检查、服务暴露、扩缩、升级和故障诊断。

vLLM、HAMi、Ray/KubeRay、LeaderWorkerSet 或其他 operator 都是可选择的实现与适配对象，不是 Eruun 的领域实体。公共契约应表达所需能力，具体 adapter 再把能力映射到已安装平台。任何依赖 CRD 的方案都必须先完成能力发现，并在缺失时明确失败。

### 3.5 Agent 评测

评测与用户自定义任务使用统一的 Job 模型，通过一个 Job 类型区分，并在所属空间的同一 namespace 中执行。这里的 Job 是 Eruun 的任务执行单位；两类任务的 Kubernetes 载体都是按类型和输入选择指定执行镜像后创建的 Deployment，而不是 Kubernetes `batch/v1 Job`。评测需要版本化数据集、目标 Agent/模型引用、确定性评分器、可选 Judge、进度、用量、延迟、逐 case 结果、报告和质量门禁。

评测的输入校验、执行配置和结果解释由其 Job 类型表达，持久化、调度和生命周期继续使用统一 Workflow/Job 链路。公共入口与可抢占协议的具体设计，要在最小实现和真实负载验证后决定；Job 类型边界见下文 4.2。

### 3.6 数据、向量化与云平台

向量化是“读取数据 → 解析和切分 → 调用 embedding → 写入目标存储 → 产出可审计摘要”的批处理能力。Eruun 不应把某个文档库、embedding 服务或向量数据库写死为产品边界。

云平台集成用于提供模型 API、对象存储、GPU/计算资源和其他 AI 服务。现有 CloudJob 可以作为异步外部副作用和 checkpoint 的实现参考，但当前内置能力不等于通用 AI Provider。Provider 设计必须覆盖凭据隔离、能力发现、幂等、状态恢复、取消/补偿、成本与审计。

## 4. 控制面与数据面边界

```mermaid
flowchart LR
    User[用户或平台] --> API[API role]
    API --> DB[(MySQL\n状态与 ownership)]
    Scheduler[Scheduler role] --> DB
    Scheduler --> Queue[Redis Streams 或 Kafka]
    Queue --> Worker[Worker role]
    Worker --> K8s[Kubernetes workloads]
    Worker --> External[模型、MCP、对象存储、云 API]
    Controller[Controller role] --> K8s
    Controller --> DB
```

- API 负责身份、授权、输入校验和任务持久化，不直接执行 Agent 工具。
- Scheduler 负责 Workflow Run 的派发和过期租约恢复；消息队列不拥有任务状态。
- Worker 执行 Workflow/Job，并通过 generation/token 防止旧执行覆盖新状态。
- Kubernetes 承载容器和资源隔离；外部系统的副作用仍需幂等键或补偿，不能宣称 exactly-once。
- Controller 观察 Kubernetes 并更新运行状态，不替代 Worker 的执行控制。

### 4.1 任务执行身份与应用归属

当前实现提供两种参考：应用 Workflow 执行会生成 `WorkflowQueue.TaskID`，其中多个 Job 共享这个 TaskID，各自以 `JobInfo.ID` 标识执行明细；[资源导入扫描与纳管任务](import-existing-namespace-api.md)则已经能够不绑定 AppID，直接保存 WorkspaceID 并生成 TaskID。对应实现见 [Workflow 创建](../pkg/apiserver/domain/service/workflow/workflow.go)、[Job 模型](../pkg/apiserver/domain/model/job.go)和[资源导入提交](../pkg/apiserver/resourceimport/jobs.go)。资源导入有专用的构建、执行和授权路径，这不代表任意 Job 已可通过通用入口省略 AppID。

以下是评测、向量化及其他独立批处理任务应遵循的设计原则，不是新增 API、表结构或 Runner 环境变量契约：

| 信息 | 设计要求 | 语义与边界 |
| --- | --- | --- |
| TaskID | 每次新任务执行必需，由服务端生成，持久化成功后返回 | 标识一次整体执行，用于状态、取消、日志和结果关联；不是 Workflow 定义 ID，也不是每个子 Job 单独生成的 ID |
| WorkspaceID 与调用者身份 | 必需；由服务端根据认证和授权上下文确定 | 独立任务直接持久化空间归属，供后台调度、授权、配额和清理使用；不能依赖 AppID 反查或相信未经授权的客户端值 |
| AppID | 应用工作流必需；独立任务不强制要求 | 表达任务的应用归属；不能为了满足执行器字段而创建占位应用 |
| WorkflowID、组件及其他业务信息 | 按任务语义提供 | 独立任务不应被迫创建持久化 Workflow 定义、Component，或填写占位 ProjectID/ProductID；任务输入仍须完整、版本化且可校验 |
| Job 执行明细身份 | 复用现有 Job 身份机制 | 一个 TaskID 可以关联多个 Job；不因为评测或向量化就增加平行的顶层 Task、Run 或专用队列表 |

同一任务的执行重试或租约恢复沿用 TaskID，由现有 generation/token 与 Job attempt 管理执行所有权和尝试，不能借恢复重置任务预算或截止时间；用户主动再次运行，即使输入相同，也属于新的 TaskID。如果提供提交幂等能力，同一个已接受请求的重复提交应返回原 TaskID，作用域和输入冲突规则必须在实现中明确。TaskID 本身不保证外部副作用只发生一次，也不是访问凭据。

独立任务可以通过目标引用关联某个应用及其版本，但该引用不改变任务的空间归属。当前模型中的 AppID 用于应用所有权，不能直接把它当作无生命周期影响的展示关联；目标引用的具体承载方式留给实现验证。任务操作须校验空间权限，执行时还须校验实际访问的目标、数据和凭据；缺少有效空间归属时应拒绝执行，不能退到默认空间。

生命周期应跟随归属：应用工作流继续遵循现有应用删除与任务取消规则；独立任务只清理本次执行拥有的资源，不能因取消任务而清理被引用应用。引用的应用被删除或失去访问权限时，应明确报告目标不可用或无权访问，并按任务策略收敛；已产生的结果仍由任务所在空间和制品保留策略管理。任务的空间归属不能通过修改目标引用而改变。

独立任务仍受空间生命周期约束。当前空间删除会拒绝未完成任务并清理终态任务记录；新提交路径须与空间删除协调，防止在已删除空间中创建任务，并验证外部制品的保留与清理。资源导入提交时锁定空间记录是现有实现参考。

实现独立任务需要逐项检查现有执行链路中依赖 AppID 的入口校验、Job 构建、空间解析、持久化授权、调度准入、状态查询、回调和清理。现有调度器、队列与状态机应继续复用，子 Job 仍须关联持久化父任务，并遵循已有 ownership/fencing、调度准入及各执行路径的 deadline 规则；不能仅放宽请求字段校验，就宣称已支持无应用任务。TaskID 是否传入 Runner、采用何种传输方式，应由最小执行协议验证，不预先承诺 `APP_ID` 或 `TASK_ID` 环境变量。

### 4.2 同一命名空间中的 Job 类型

目标设计以 Job 的 `type` 作为区分 Agent 评测与用户自定义任务的唯一分类。同一空间中的两类任务都由对应 Job 控制器在该空间已确定的 namespace 中创建任务执行 Deployment：控制器按类型和输入选择指定执行镜像，由该镜像创建并运行一次性任务。两类任务共用执行记录模型、调度器、队列、取消、超时、重试和清理机制；不按类型另建 namespace 或任务实体。不同空间仍保持隔离，namespace 由服务端根据授权空间解析，不能通过切换类型或任意填写 namespace 越过空间边界。首次运行所需的 namespace 就绪与安全基线也应由空间路径完成，不依赖创建占位应用。Deployment 的名称、selector、追踪元数据和清理条件必须绑定所属 TaskID 与 Job 身份，不能依赖空 AppID，也不能与同一 namespace 中的其他任务发生碰撞或交叉清理。

以下类型值只是概念示例，尚未注册或冻结为 API 枚举：

| Job `type` 示意 | 类型负责的差异 | 共用执行边界 |
| --- | --- | --- |
| `agent_evaluation` | 校验目标、数据集和评分配置，选择评测执行镜像，解释指标、报告和 verdict | 所属空间的 namespace、TaskID/Job 身份、Deployment 执行与生命周期 |
| `custom` | 校验用户声明的镜像、命令、参数及输入输出，选择自定义任务执行镜像并记录退出结果与制品 | 所属空间的 namespace、TaskID/Job 身份、Deployment 执行与生命周期 |

镜像、命令、资源和凭据引用等容器执行信息应复用共用能力；类型决定必要的输入规则、执行镜像、Deployment 运行配置和结果语义，不引入第二套执行器生命周期。用户自定义的是任务内容，不是任意注册控制面类型或调用内部清理、回调等 Job。提交时应校验允许使用的类型及相应输入；缺失、未知或无权使用的类型明确拒绝，不能静默按 custom 执行。接受后，Job 类型须随执行记录持久化并在恢复时保持一致。

现有代码已有 [config.JobType](../pkg/apiserver/config/consts.go)，通过 `JobTask.JobType` 选择 [Job 控制器](../pkg/apiserver/event/workflow/job/job.go)，并写入 `JobInfo.Type`。实现应优先扩展这条类型链路，让两类 Job 复用 Deployment 的创建、观察、重试和清理基础；不再增加含义重复的 category、purpose 或评测标记。`WorkflowQueue.Type` 的 `WorkflowTaskType` 表达父任务编排用途，不承载这两类 Job 的重复分类；同一个 TaskID 下可以按需编排不同类型的 Job。Eruun Job 类型与 Kubernetes 资源 `kind` 是不同概念；两类任务的底层资源均为 Deployment。

Deployment Ready 只表示执行载体就绪，不能表示其中的一次性任务已经完成。执行镜像必须通过后续实现确定的结果协议上报进度和终态，控制器只接受当前 TaskID、Job 身份及执行代的结果；成功、失败、取消或超时收敛后，应按任务生命周期停止并清理对应 Deployment。若执行镜像的主进程会在任务完成后退出，实现还必须避免 Deployment 将其自动重启为新的执行。

当前 `instant_job` 等类型参与调度准入、延迟执行、结果恢复、重试和清理的判断，不能只新增枚举和分发分支就认为接入完成。新增类型需核对上述路径以及无 AppID 的空间解析和持久化授权，并通过同一 namespace 内混合运行两类 Job 的验收。具体枚举名称、请求与存储映射由实现 PR 确定，但单一 Job 类型分类与共用执行边界是本草案的设计选择。

## 5. 路线图与进入条件

### Phase 1：自托管 Agent 基线

- 选定最小 Agent 执行场景，验证现有组件与 Workflow 是否足够表达。
- 以容器镜像、命令、输入输出、生命周期、资源和终止语义完成单集群闭环。
- 在 namespace 隔离和 Restricted Pod Security 下验证部署、执行、取消和清理。

### Phase 2：权限与工具

- 明确 MCP/CLI 的运行位置、工具 allowlist、Secret 引用、出站网络、审计和审批边界。
- 对平台授权、Kubernetes 身份、容器权限、数据凭据、网络访问和工具调用分别实施默认拒绝策略。

进入下一阶段前，必须能够回答一次工具调用由谁授权、使用什么身份、访问什么资源、产生什么审计记录，以及失败或取消后如何收敛。

### Phase 3：评测与可观测性

- 以一个小型、确定性数据集完成提交、执行、进度、报告和质量门禁闭环。
- 在同一空间 namespace 中同时运行 Agent 评测和用户自定义 Job，验证由一个类型区分、各自创建任务执行 Deployment、共用调度与生命周期且执行资源互不混淆。
- 验证不创建 Application 的独立评测能够生成 TaskID、持久化空间归属并完成调度、取消和清理；应用工作流中的评测保留原有归属与生命周期。
- 验证敏感输入与逐 case 制品的访问控制、保留和删除策略。
- 用最小实现确定 Job 类型的枚举与输入映射，用真实数据决定是否需要进一步扩展 API 或抢占能力。

### Phase 4：模型服务与 GPU

- 先支持单节点、单 Pod 的模型服务，再根据真实模型规模选择多节点 adapter。
- 增加通用资源、调度和 operator-backed workload 能力时，不以 vLLM 或某个 GPU 插件命名公共实体。
- 以固定模型 revision、服务就绪、失败诊断、更新和清理作为验收闭环。

### Phase 5：数据流水线与云 Provider

- 让向量化和云动作复用统一任务状态、凭据边界与制品治理。
- 从一个真实 Provider/Action 开始验证通用边界，不先建设无调用者的插件市场或热插拔框架。
- 多集群、跨地域和复杂成本调度只有在单集群闭环稳定后再进入设计。

## 6. Proposal 的写作和升级规则

- Proposal 必须在标题后直接标记 `Draft / Proposal`，并列出当前可复用能力与缺口。
- 未实现的路由、环境变量、JSON 字段、表字段、默认值和时间参数不得写成既定契约；必须删除或明确标成不具约束力的概念示例。
- Current 文档中的命令和请求必须能从 `main` 的代码、配置或测试找到事实源。
- 专题设计达到 Current 前，至少需要实现、测试、运维和安全证据，并同步文档索引。
- 外部项目的参数和资源名要标注核验版本/日期，不能直接升级为 Eruun 的稳定 API。

## 7. 外部事实核验

本文于 2026-09-04 核验以下官方资料；它们会继续演进，实施时仍需重新确认：

- [MCP Authorization 2025-06-18](https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization)：HTTP 授权、资源受众约束和禁止 Token passthrough。
- [MCP Tools draft](https://modelcontextprotocol.io/specification/draft/server/tools)：工具发现、调用与用户控制的协议边界。
- [vLLM serve CLI](https://docs.vllm.ai/en/latest/cli/serve/) 与 [vLLM online serving](https://docs.vllm.ai/en/stable/serving/online_serving/)：服务参数和分布式执行后端持续演进，Eruun 不固定唯一后端。
- [HAMi configuration v2.5.1](https://project-hami.io/docs/v2.5.1/userguide/configure)：GPU、显存和核心资源名属于该版本的平台配置，不是 Eruun 固有字段。
