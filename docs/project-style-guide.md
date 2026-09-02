# Eruun 项目风格指南

> 状态：Implemented Reference。本文总结当前代码库的命名规范、模块边界和架构设计风格，作为后续代码、文档和评审的维护基线。

## 1. 项目定位


当前代码风格的核心取向是：

- API Server 负责 HTTP 契约、服务装配、中间件和运行期生命周期。
- Domain 承载应用、工作流、系统设置、校验、转换和导入等业务规则。
- Workflow/Event 承载任务调度、队列消费、状态机推进和 Job 执行。
- Infrastructure 承载 MySQL、Redis、Kafka、Kubernetes、Informer、Locker、Tracing 等外部系统适配。
- OAM-style traits 负责把声明式组件扩展转换成 Kubernetes workload 变更。

本文描述的是当前实现风格，不替代具体 API、字段和运行契约文档。涉及跨层字段语义时，仍以 `core-module-boundary-and-cross-layer-contracts.md` 为准；涉及工作流执行时，仍以 `workflow-architecture-guide.md` 为准。

## 2. 模块组织风格

| 模块 | 当前职责 | 风格要求 |
| --- | --- | --- |
| `cmd/main.go` | API Server 主入口 | 保持薄入口，只做全局注册、命令构建和错误退出 |
| `cmd/server/app` | 服务端 Cobra 命令、参数、启动生命周期 | 配置覆盖、校验和 server 运行应 fail-fast；初始化错误应携带操作上下文 |
| `pkg/apiserver/interfaces/api` | 路由、请求绑定、响应封装、中间件 | Handler 只做 HTTP 契约处理和 Domain 委托，不直接写 DB 或 K8s |
| `pkg/apiserver/interfaces/api/dto/v1` | API 请求和响应结构 | DTO 表达对外 JSON 契约，字段变化必须同步 assembler、examples 和 docs |
| `pkg/apiserver/interfaces/api/assembler/v1` | Domain 到 DTO 的组装 | 负责展示字段、派生字段、兼容字段和脱敏，不放持久化或 K8s 调用 |
| `pkg/apiserver/domain/model` | GORM 模型和领域实体 | 模型字段是 DB 主事实源之一，字段语义要和跨层契约文档一致 |
| `pkg/apiserver/domain/repository` | 仓储接口和数据访问意图 | 新代码优先使用接口表达业务查询/写入意图，兼容函数保留但不扩大使用面 |
| `pkg/apiserver/domain/service` | 领域服务 | 应用生命周期、校验、转换、导入、workflow 创建等规则集中在这里 |
| `pkg/apiserver/domain/spec` | 共享规格和值对象 | Traits、系统设置、安全策略等跨 DTO/Domain 的语义结构优先放这里 |
| `pkg/apiserver/event/workflow` | 工作流调度和控制器 | 以 DB 状态机为事实源，队列只承载分发，控制器负责状态推进和 ack |
| `pkg/apiserver/event/workflow/job` | Kubernetes 资源 Job 控制器 | 每类资源独立控制器，生成、应用、等待、清理语义要保持一致 |
| `pkg/apiserver/event/workflow/cloudjob` | 云资源 Provider 合约 | Provider、Action、上下文和状态字符串集中定义，避免散落魔法字符串 |
| `pkg/apiserver/workflow/traits` | Trait Processor | Processor 有序注册、无状态、返回聚合结果，由框架统一应用到 workload |
| `pkg/apiserver/workflow/config` | 工作流运行配置和共享策略 | 集中运行参数、默认值、校验、失败/运行策略和回调超时规则；不依赖全局配置、领域模型或执行器 |
| `pkg/apiserver/workflow/naming` | Kubernetes 资源命名 | 所有资源名生成优先复用这里，避免局部拼接造成清理和同步不一致 |
| `pkg/apiserver/infrastructure` | 外部系统适配 | 实现连接、队列、存储、Informer、锁和可观测性，不反向承载业务规则 |
| `pkg/apiserver/utils` | 技术工具 | 只放可复用技术 helper，不放应用生命周期、workflow 或 API 业务分支 |

模块之间的默认方向是 `interfaces -> domain -> infrastructure`，workflow/event 由 domain 创建任务并由 worker 异步执行。跨层依赖应保持单向、显式和可测试。

## 3. 命名规范

### 3.1 Go 命名

- 包路径使用小写短名，例如 `application`、`workflow`、`namespaceimport`、`systemsetting`。
- 导出类型、接口和函数使用 PascalCase，例如 `ApplicationsService`、`WorkflowService`、`NewWorkflowController`。
- 非导出实现使用 camelCase，例如 `applicationsServiceImpl`、`workflowUpsertOptions`、`buildComponentServices`。
- 接口名优先表达领域能力，例如 `ApplicationRepository`、`NamespaceImportService`、`TraitProcessor`。
- 实现结构体通常以功能名加 `Impl` 或具体控制器命名，例如 `applicationsServiceImpl`、`DeployRoleJobCtl`、`CallbackJobCtl`。
- 构造函数使用 `NewXxx`，注册函数使用 `RegisterXxx` 或 `InitXxxBean`。
- 测试函数使用 `Test目标_场景` 或 `Test目标场景`，场景名应说明行为结果，例如 `TestApproveWorkflowTaskRejectsInvalidAction`。

### 3.2 API、JSON 和字段命名

- 外部 JSON 字段以 camelCase 为主，例如 `appId`、`componentName`、`templateEnabled`、`readyReplicas`。
- 路径参数沿用当前 API 风格，例如 `:appID`、`:componentName`、`:taskID`。
- 请求/响应结构放在 `dto/v1`，不要把 Domain model 直接作为新的 API 契约暴露。
- Handler 参数绑定和校验失败应走统一响应封装和错误码路径。

### 3.3 DB、缓存和 Kubernetes 命名

- DB 表和列使用 snake_case，GORM tag 明确列名，例如 `app_id`、`component_type`、`ready_replicas`。
- 模型 JSON tag 可以保持 API 可读形态，但 DB tag 必须明确持久化字段。
- 缓存键使用语义化前缀和版本号，例如 `app:components:v6:<appId>`，缓存不是事实源。
- Kubernetes label、annotation 和资源名必须复用 `config` 常量和 `workflow/naming` helper。
- Kubernetes 资源名按 RFC1123 归一化，并在超长时使用稳定 hash 后缀，避免局部截断导致资源漂移。

### 3.4 常量和状态命名

- 当前跨模块共享的 `JobType`、`Status`、`WorkflowMode`、`WorkflowStepType` 等枚举仍在 `pkg/apiserver/config`。
- 工作流的 `JobRunPolicy`、`WorkflowFailurePolicy` 及其归一化规则由 `workflow/config` 管理；运行配置使用该包的 `RuntimeConfig`、默认值和校验。全局 `config.Config.Workflow` 组合模块配置，并保留现有 flags 和 `ERUUN_` 环境变量入口。
- 系统设置、traits 和安全策略的规格结构放在 `domain/spec`，避免 DTO 和 Domain 重复定义同一语义。
- 新增状态必须明确所在状态机、终态/非终态属性、回写路径和 API 展示语义。

## 4. 架构设计风格

### 4.1 分层委托

API handler 的职责是接收 HTTP 请求、绑定参数、调用领域服务并封装响应。业务规则应下沉到 Domain Service，持久化细节应落在 Repository 或 DataStore，外部系统调用应落在 Infrastructure 或 Job 控制器。

推荐路径：

1. HTTP 路由在 `interfaces/api` 注册。
2. 请求/响应结构在 `dto/v1` 定义。
3. Handler 调用 `domain/service` 接口。
4. Service 组合 repository、datastore、cache、workflow 或 infrastructure adapter。
5. Assembler 将 Domain 模型转换为 API 响应。

不推荐路径：

- Handler 直接拼 DB 查询、Kubernetes client 调用或 workflow 状态机逻辑。
- Assembler 写入 DB、读取 Kubernetes 或触发副作用。
- `utils` 承载某个业务分支的专用规则。

### 4.2 Domain Service 风格

Domain Service 是业务规则的主要归属地。当前应用服务拆分为 create、delete、query、version update、template clone、component ops 等同包文件，保持公共接口稳定，用私有 helper 分解复杂流程。

服务方法通常具备以下特征：

- 第一个参数是 `context.Context`。
- 输入使用 DTO 或领域参数对象，输出返回 DTO 或 Domain model。
- 跨表写入优先使用事务。
- 业务前置条件显式检查，失败时直接返回带上下文的错误。
- 需要默认值时在边界处明确设置，不做不可见的降级替代。

### 4.3 Repository 和 DataStore 风格

Repository 接口表达业务查询和写入意图，DataStore 提供底层 CRUD 和事务能力。当前代码保留了一些历史函数式 repository helper，新代码应优先向接口方法收敛，避免把上层 DTO 或 HTTP 语义传入 repository。

Repository 层应做到：

- 接收 `context.Context`。
- 返回 Domain model 或领域错误。
- 不拼接 API 响应结构。
- 不承载跨资源业务编排。

### 4.4 Workflow/Event 风格

工作流运行时以 `WorkflowQueue` 和 `JobTask` 状态为核心事实源。Dispatcher 负责扫描和投递任务，Worker 消费队列，`WorkflowCtl` 推进步骤状态，Job 控制器执行具体资源操作。

设计风格包括：

- Workflow controller 使用任务快照、互斥保护和 ack 回写。
- 队列用于分发和恢复，不替代 DB 状态机。
- Step 支持串行和并行模式，资源按优先级执行。
- Job controller 统一实现 `Run`、`Clean`、`SaveInfo` 语义。
- Job 基类抽出 namespace、job、client、store、ack、locker、waiter 等公共运行依赖。
- 取消、审批、超时和回调都要明确状态转换和持久化点。

### 4.5 Trait 风格

Traits 是组件声明到 Kubernetes workload 变更的扩展层。当前框架使用有序全局注册、反射定位 trait 字段、Processor 处理单一 trait，并把结果聚合后统一应用。

新增或调整 Trait 时应保持：

- `domain/spec` 定义用户可见结构。
- `workflow/traits` 实现无状态 `TraitProcessor`。
- 在 `RegisterAllProcessors` 中明确执行顺序。
- 嵌套 trait 要明确递归和排除规则，避免无限递归。
- 生成的 `AdditionalObjects`、env、volume、service account、probe、resource 等都通过 `TraitResult` 表达。
- API 示例和专题文档说明用户输入形态。

### 4.6 Infrastructure 风格

Infrastructure 只适配外部系统，包括 Kubernetes client、Redis、Kafka、MySQL、Informer、Locker、Tracing。它可以实现接口、封装 SDK、提供重试或连接健康检查，但不应决定应用生命周期、组件状态优先级或 API 展示语义。

配置错误、连接错误和不支持的后端应 fail-fast。运行期可恢复错误要可观测，并由调用方决定是否继续。

## 5. 编码倾向

| 主题 | 当前倾向 |
| --- | --- |
| 错误处理 | 使用 `fmt.Errorf("operation: %w", err)` 包装操作上下文；不要吞错 |
| 初始化 | 配置、依赖和外部后端不可用时 fail-fast |
| Context | 对外方法和 goroutine 入口传入 `context.Context`，避免无理由使用 `context.Background()` |
| 日志 | 新代码优先使用 `klog.InfoS`、`klog.ErrorS` 或 context logger 的 key/value 形式 |
| 并发 | 使用 mutex、atomic、context cancel、worker group 或队列 ack 明确并发边界 |
| 事务 | 应用创建、workflow upsert、跨表状态更新等要在事务边界内完成 |
| 缓存 | 缓存是加速层，DB 是主事实源；缓存失败不能改变主流程正确性 |
| 默认值 | 默认值集中在所属模块的配置包或边界归一化 helper；工作流运行和回调超时默认值由 `workflow/config` 管理 |
| 兼容 | 历史字段和行为可以保留，但新增能力不要扩大旧契约负担 |
| 注释 | 只解释非显然约束、状态机原因和跨层契约，不重复代码本身 |

项目整体偏向显式、可追踪、可回滚的实现。除转换/导入等已声明 best-effort 的能力外，不默认引入静默 fallback 或降级路径。

## 6. 测试风格

当前测试以包级单元测试为主，文件与目标代码同目录，测试命名强调行为和场景。

新增测试时优先：

- 使用 table-driven cases 覆盖输入矩阵。
- 覆盖错误路径、边界值、状态转换和历史兼容字段。
- 对 API handler 使用 fake service 验证绑定、响应和错误码。
- 对 Domain service 使用 fake/in-memory store 或测试 helper 验证业务规则。
- 对 Workflow/Job 使用 fake Kubernetes client、fake queue、fake waiter 验证状态推进。

测试范围按风险选择：

- 单一 helper 或 DTO 变化：包级测试即可。
- 跨 API/Domain/Assembler 字段变化：补 handler/service/assembler 回归。
- Workflow、状态机、队列、K8s 资源语义变化：补 event/job 级测试，必要时运行更广范围。
- Docs-only 变化：做文档和链接审查即可，不需要运行 `go test`。

## 7. 文档维护风格

`docs/` 中的专题文档通常使用以下格式：

1. 一级标题。
2. `> 状态：...` 状态行。
3. 背景、当前事实、契约、示例或实现锚点。
4. 必要时提供 Mermaid 图、表格、请求/响应示例和检查清单。

新增或修改行为时应同步：

- API 或字段变化：更新专题 API 文档、`examples/` 和 `docs/README.md`。
- Workflow 语义变化：更新 `workflow-architecture-guide.md` 或对应专题文档。
- 跨层字段变化：更新 `core-module-boundary-and-cross-layer-contracts.md`。
- 重要架构取舍、迁移或兼容风险：新增或更新 `devlogs/` 决策记录。

文档描述代码事实，不替代码补偿行为。如果文档和代码冲突，先以代码为准，再修正文档。

## 8. 常见变更落点

| 变更类型 | 优先修改位置 | 同步检查 |
| --- | --- | --- |
| 新 API | `interfaces/api`、`dto/v1`、`assembler/v1`、`domain/service` | examples、API docs、错误码、handler tests |
| 新应用业务规则 | `domain/service/application` | repository 语义、事务、缓存失效、service tests |
| 新 workflow 行为 | `domain/service/workflow`、`event/workflow` | 状态机、队列 ack、超时/取消、workflow docs |
| 新 Kubernetes 资源 Job | `event/workflow/job` | 资源命名、wait/cleanup、JobType、job tests |
| 新 Trait | `domain/spec`、`workflow/traits` | 注册顺序、TraitResult、示例、trait tests |
| 新外部后端 | `infrastructure`、`config` | 配置校验、fail-fast、health/ready、docs |
| 新共享 helper | 目标包内私有 helper 优先，必要时再放 `utils` | 是否真正跨包复用，是否夹带业务规则 |

## 9. 评审检查清单

- 新代码是否放在现有模块边界内，而不是新增不必要实体或包。
- Handler 是否只处理 HTTP 契约，业务规则是否下沉到 Domain。
- DTO、Domain model、DB 列、缓存键和 K8s label/annotation 是否命名一致且有事实源说明。
- 新状态是否明确状态机、终态、回写点和 API 展示。
- 新资源名是否复用 `workflow/naming`，新常量是否归属对应模块的配置包。
- 错误是否带操作上下文，是否避免吞错和静默降级。
- 是否传递 `context.Context`，goroutine 是否有退出路径。
- 缓存是否只做加速，不成为事实源。
- 是否补充了最小但有效的测试和文档。
- Docs-only 变更是否更新 `docs/README.md` 索引。
