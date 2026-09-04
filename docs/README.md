# Eruun 文档索引

> 状态：Current。本文是 `docs/` 的导航入口，按当前 `main` 代码事实整理文档状态，并为维护者和 LLM 提供需求定位路径。

## 产品方向与能力成熟度

Eruun 的长期方向是面向 Agent、模型和 AI 工作负载的分布式运行时；当前可用产品仍是 Kubernetes Application/Workflow Runtime。文档中的愿景不能覆盖代码事实。

| 阶段 | 能力 | 文档解释 |
| --- | --- | --- |
| Current | Application、Component、Traits、Workflow、四角色运行时、Kubernetes 调和、认证与空间 | 可按文档直接使用，必须与 `main` 一致 |
| Next | Kubernetes 自托管 Agent、MCP/CLI 工具边界、凭据/权限、审计、Agent 评测 | 方向已明确，公共契约尚未冻结 |
| Later | 模型服务、GPU 感知调度、向量化、托管 AI Provider、云或多集群能力 | 探索阶段 |

[AI Runtime 愿景](ai-runtime-vision.md) 是未来方向的唯一总纲。任何专题 Proposal 中出现的名称或示例，除非另有 Current 实现和测试，不构成 API、JSON、数据库或部署承诺。

## 推荐阅读顺序

1. `AGENTS.md`：仓库协作规则、构建测试命令、提交和 PR 要求。
2. 本文档：目录分层、需求定位、当前代码事实和文档索引。
3. `架构文档.md` 与 `architecture-diagrams.md`：当前角色、数据、Component、Trait 和 Workflow 边界。
4. `core-module-boundary-and-cross-layer-contracts.md`：API、Domain、DB、Cache、K8s 的核心字段契约。
5. `workflow-architecture-guide.md`：工作流调度、队列、Job 执行和状态机。
6. `enterprise-distributed-runtime-design.md`：四类运行角色、双 Leader、Workflow 数据库租约和部署拓扑。
7. `ai-runtime-vision.md`：AI Runtime 目标、路线图和 Proposal 进入实现的门禁。

## 状态约定

- `Current`：面向用户的当前可用能力说明，应与代码和命令输出保持一致。
- `Implemented Reference`：已实现能力的设计/行为参考，可能比快速入门更细。
- `Draft / Proposal`：设计草案或后续演进方案，不代表当前已暴露能力。
- `Historical / Audit`：历史备案、审计或测试记录，用于追溯，不作为最新使用手册。
- `Deprecated`：兼容或迁移参考，不作为新接入、新功能或当前主路径的依据。

## LLM / CONTEXT / ADR 使用规则

- `Implemented Reference` 可用于理解实现背景和内部结构，但写入 CONTEXT、ADR 或对外说明前，必须用代码、命令输出或 `Current` 专题文档二次确认。
- `Draft / Proposal`、`Historical / Audit`、`Deprecated` 只能作为设计背景、迁移线索或审计证据，不得改写为当前已暴露能力。
- Proposal 中的路由、字段、默认值和伪代码不得用于生成客户端或部署；只有 Current 文档和代码才是可执行契约。
- `devlogs/` 记录一次 PR/主题的当时决策，可能被后续 devlog 或 `Current` 文档 supersede；当前事实以代码与 `docs/` 中的 `Current` 文档为准。

## 当前代码事实速查

- API 前缀：`/api/v1`。
- 服务进程本地默认监听：`127.0.0.1:8001`；`deploy/eruun-stack.yaml` 会通过 `ERUUN_BIND_ADDR=0.0.0.0:8000` 覆盖该默认值并暴露集群内服务。
- MySQL 默认 DSN 是 `127.0.0.1:3306/eruun` 的本地连接模板，必须替换密码占位符；Kafka 默认 Broker 为 `localhost:9092`，消息后端仍默认 Redis。字段与覆盖方式见 [`config/apiserver-default.yaml`](../config/apiserver-default.yaml) 和 [Kafka 配置说明](kafka-queue-implementation.md#4-配置说明)。
- 服务端角色通过 `--role` / `ERUUN_ROLE` 显式选择 `api/controller/scheduler/worker`；直接运行默认是 `api`，不存在聚合 `all` 角色。
- Workflow 固定使用 v2 generation/token ownership 与数据库执行租约，Worker 不再获取 Redis 执行锁；不存在关闭 fencing 或处理 v1 dispatch 的运行模式。
- 顶层 `/workflow`、`/workflow/exec`、`/workflow/cancel` 路由不再注册；应用维度 workflow API 是当前主路径。
- 业务 API 强制 Bearer 登录并按个人/团队空间授权；账号配置由 `ERUUN_AUTH_CONFIG_FILE` 加载，所有认证依赖失败时保持拒绝访问。
- 应用必须属于一个空间；namespace 在首次实际部署时初始化，账号注册和应用保存不创建 Kubernetes 资源。
- 当前没有 Agent、MCP、Agent evaluation、向量化、vLLM/HAMi 或通用托管 AI Provider 公共 API。

## 目录层级速查

| 路径 | 职责 | 常见需求入口 | 注意事项 |
| --- | --- | --- | --- |
| `cmd/main.go`, `cmd/server/app` | API Server 启动、参数、服务装配 | 新增启动参数、调整初始化顺序 | 配置问题优先 fail-fast，不要静默降级 |
| `pkg/apiserver/adoption` | 既有 Kubernetes 资源接管的共享契约 | adoption snapshot、资源 identity/digest、StatefulSet 重启安全校验 | 只放跨导入、生命周期与 workflow 的规则；Kubernetes 客户端适配仍在 `infrastructure/adoption` |
| `pkg/apiserver/interfaces/api` | HTTP 路由、参数绑定、响应封装、中间件 | 新接口、接口校验、认证授权、流式能力 | 不直接写 DB/K8s，业务逻辑下沉到 Domain |
| `pkg/apiserver/interfaces/api/dto/v1` | API DTO 与请求/响应结构 | 字段增删、响应形态调整 | 同步 assembler、文档和 examples |
| `pkg/apiserver/interfaces/api/assembler/v1` | Domain 对象到 DTO 的组装 | 响应字段推导、脱敏、兼容字段 | 不放持久化或 K8s 调用逻辑 |
| `pkg/apiserver/domain/model` | GORM 模型和领域实体 | 新表字段、状态字段、业务实体 | 字段语义必须同步跨层契约文档 |
| `pkg/apiserver/domain/repository` | 仓储接口和数据访问契约 | 查询/写入方法、事务边界 | 接口表达业务意图，不暴露上层 DTO |
| `pkg/apiserver/domain/service` | 应用生命周期、转换、查询、工作流创建 | 创建/更新/删除应用、组件查询、K8s YAML 转换 | 保持业务规则集中，避免依赖接口层细节 |
| `pkg/apiserver/domain/spec` | 共享规格、资源契约、策略和校验 | Auth、OAuth、URL 安全、云资源配置、资源类型、Service 暴露类型与共享策略 | 业务取值及归一化与对应规格集中定义 |
| `pkg/apiserver/event/workflow` | Workflow 调度、分发、状态推进、审批/超时 | 任务状态、队列消费、分布式执行 | DB 状态机是任务事实源 |
| `pkg/apiserver/event/workflow/job` | 具体 Job 控制器和 K8s 资源调和 | Deployment、StatefulSet、Service、PVC、Secret、RBAC 等资源执行 | 保持资源生成、等待和清理语义一致 |
| `pkg/apiserver/event/workflow/cloudjob` | 云资源 Provider 合约与实现 | 云资源步骤、Provider 注册、外部云动作 | 合约字符串集中为常量 |
| `pkg/apiserver/workflow/traits` | OAM Traits 处理器 | storage、env、probe、resources、sidecar、rbac、ingress 等 Trait | 新 Trait 需要处理顺序、测试和文档 |
| `pkg/apiserver/workflow/config` | 工作流运行配置、执行策略与 topic 命名 | 调度/Worker 默认值、配置校验、回调超时、镜像拉取策略与队列名称 | 模块配置不反向依赖全局配置、领域模型或执行器 |
| `pkg/apiserver/workflow/naming` | 资源命名规则 | Kubernetes 资源名、PVC/Service 命名 | 命名变化影响状态同步和清理 |
| `pkg/apiserver/infrastructure` | 外部系统与安全机制适配 | K8s、Redis、Kafka、MySQL、Informer、锁、可观测性、adopted Secret 加密 | Infrastructure 实现接口，不反向承载业务规则；导入 Secret 的加密/签名位于 `infrastructure/importsecret` |
| `pkg/apiserver/utils` | 通用工具 | 缓存、错误码、异步执行、K8s helper、profiling | 新工具必须可复用，避免放业务分支 |
| `pkg/apiserver/config` | 进程配置入口与模块配置组合 | 启动参数、环境变量、连接配置、模块配置装配 | 模块专属策略和资源契约由所属模块定义 |
| `config`, `deploy`, `examples`, `scripts` | 默认配置、部署清单、请求样例和辅助脚本 | 部署参数、示例更新、脚本化验证 | 行为或配置变化要同步 docs |

## 需求定位表

| 需求类型 | 先看哪里 | 常改哪里 | 必查文档 |
| --- | --- | --- | --- |
| 新增或修改 API | `pkg/apiserver/interfaces/api` | handler、DTO、assembler、domain service、examples | `validation-api-guide.md`, `core-module-boundary-and-cross-layer-contracts.md` |
| 应用创建/更新/删除流程 | `pkg/apiserver/domain/service/application*.go` | domain service、repository、workflow 触发点 | `create-and-exec-application-api.md`, `version-update-api.md`, `database-reset-workflow.md`；`reset-workflow.md` 仅作兼容/废弃参考 |
| 工作流状态、队列或并发 | `pkg/apiserver/event/workflow` | dispatcher、controller、queue、job 状态推进 | `workflow-architecture-guide.md`, `workflow-testing-guide.md` |
| Kubernetes 资源生成或等待 | `pkg/apiserver/event/workflow/job` | resource generation、job controller、waiter | `architecture-diagrams.md`, `statefulset-pvc-volume-naming.md` |
| Trait 能力 | `pkg/apiserver/workflow/traits` | trait processor、job builder、相关 API 示例 | `架构文档.md`, `share-trait.md`, `rollout-trait.md` |
| 组件状态、Pod 日志/文件/执行 | `pkg/apiserver/domain/service`, `pkg/apiserver/interfaces/api` | component query、pod ops、logs/files/exec API、Log archive workflow | `application-status-api.md`, `component-log-stream-api.md`, `component-pod-file-exec-api.md`, `log-archive-upload-workflow.md`, `component-container-info-api.md` |
| 认证、授权、OAuth、团队空间 | `pkg/apiserver/domain/service/account` | account、middleware、account workspace scope、infrastructure/workspace | `account-auth-workspaces.md` |
| 配置、系统设置、安全策略 | `pkg/apiserver/config`, `pkg/apiserver/domain/spec` | config defaults、validation、system setting service | `system-setting.md`, `url-security-policy.md` |
| 消息队列或分布式执行 | `pkg/apiserver/infrastructure/messaging`, `pkg/apiserver/domain/repository/workflow_lease.go` | 运行角色、Redis Streams、Kafka、workflow worker、DB lease/fencing | `enterprise-distributed-runtime-design.md`, `leader-informer-recovery.md`, `kafka-queue-implementation.md`, `workflow-architecture-guide.md` |
| Agent、MCP、评测、模型或 AI Provider 方向 | 先读 `ai-runtime-vision.md` | 先校准 Current 能力与 Proposal 门禁，再决定是否进入实现 | 对应 AI 专题 Proposal；不得把草案字段当成现有契约 |

## 当前能力入口

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `local-docker-dependencies.md` | Current | MySQL、Redis、Kafka 本地 Compose 分组、凭据、连接配置、健康检查和数据保留 |
| `distributed-runtime-hardening-merge-guide.md` | Current | 已合并的 7 个分布式运行时加固 PR、实现边界、合并记录与待完成的真实集群验收清单 |
| `account-auth-workspaces.md` | Current | GitHub/Google、邮箱/手机号登录、会话、团队权限、延迟任务隔离与失败收尾、重复部署幂等性、前端与部署接入 |
| [`../examples/account-auth-workspaces/README.md`](../examples/account-auth-workspaces/README.md) | Current | 账号与团队 API 实操：curl 注册/登录/刷新、OAuth 浏览器回调、身份绑定、邀请和空间资源访问 |
| `api-error-response-contract.md` | Current | API 统一错误响应与通用错误脱敏契约 |
| `system-setting.md` | Current | 系统设置类型、API 与默认初始化 |
| `settings-page-api.md` | Current | 前端设置页字段归属、请求参数与保存策略 |
| `programming-language-api.md` | Current | 管理员维护编程语言选项的 CRUD API |
| `url-security-policy.md` | Current | 出站 URL 安全策略 |
| `validation-api-guide.md` | Current | Try/DryRun 校验 API |
| `create-and-exec-application-api.md` | Current | 创建并执行应用 API |
| `app-workflow-callback.md` | Current | App 与 Workflow Callback 优先级 |
| `workflow-failure-policy.md` | Current | Workflow `cleanup_failed` / `cleanup_all` 策略与 Job 级失败清理例外 |
| `leader-informer-recovery.md` | Current | Controller/Scheduler 双 Leader、Informer 重建、Worker 独立观察、数据库执行租约恢复与 UTC 时钟回归验证 |
| `batch-applications-api.md` | Current | 批量应用详情查询 API |
| `application-management-mode.md` | Current | 应用 `native` / `observe` 写权限边界与历史导入迁移契约 |
| `application-status-api.md` | Current | 单应用聚合状态、单应用组件状态明细、批量应用状态的接口边界 |
| `lifecycle-api-route-migration.md` | Current | PR #209 生命周期 API 路由迁移说明，供前端客户端改造旧接口 |
| `batch-components-status-api.md` | Current | 批量应用状态汇总 API |
| `component-container-info-api.md` | Current | 组件容器信息查询 API |
| `component-log-stream-api.md` | Current | 组件日志 SSE API |
| `component-pod-file-exec-api.md` | Current | 组件文件导出与 Shell 执行 |
| `log-archive-upload-workflow.md` | Current | 组件日志归档同步下载 API、保留的 workflow jobType 与 uploader 接入边界 |
| `components-api-sidecar-field.md` | Current | Components API 字段说明 |
| `import-existing-namespace-api.md` | Current | Namespace 存量资源纳管 |
| `version-update-api.md` | Current | 版本更新 API；`executionScope` 控制 workflow task 执行范围；全量清理/全量部署使用 `remove cleanup_all` / `add all` 保留动作 |
| `version-diff-update-api.md` | Current | 版本差异更新 API；执行时透传 `/version` 的 `executionScope` |
| `database-reset-workflow.md` | Current | 指定 `store` 组件数据库 PVC 数据重置 API、异步 Workflow 执行模型和 `examples/database-reset-workflow/` 示例 |
| `reset-workflow.md` | Deprecated | 指定组件 Reset Workflow；全量 reset 入口已迁移到 `/version` |
| `workflow-approval-pause-resume.md` | Current | 审批暂停/继续 |
| `workflow-timeout-status-disambiguation.md` | Current | 超时状态区分 |
| `storage-trait.md` | Current | Storage Trait 字段契约，包含 `subPath` 与 `subPathExpr` 的语义和来源 |
| `rollout-trait.md` | Current | Rollout Trait |
| `share-trait.md` | Current | Share Trait |
| `service-trait-explicit-service-name.md` | Current | Service Trait 显式命名 |
| `kubernetes-label-normalization.md` | Current | Kubernetes label value 规范化规则 |
| `template-engine-status.md` | Current | 模板能力状态 |
| `mysql-template-init-env-example.md` | Current | MySQL 模板初始化环境变量示例 |
| `tcp-ingress-nginx-dependencies.md` | Current | Redis/MySQL TCP 外部访问配置 |
| `helm-deployment.md` | Current | Helm 四角色拓扑、schema 迁移与数据库配置、探针、PDB、topology spread、ServiceAccount、Controller Job 权限与 Quickstart 旧 RBAC 绑定清理边界 |

## 实现参考

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `workflow-architecture-guide.md` | Implemented Reference | 工作流引擎架构详解 |
| `enterprise-distributed-runtime-design.md` | Implemented Reference | 分布式运行时：角色依赖、API Redis readiness、Leader Election、数据库 lease/fencing、延迟任务恢复与通知去重、Cron 有界分页与失败计划重试、Worker observer 和 Helm 拓扑 |
| `架构文档.md` | Implemented Reference | 当前四角色、数据所有权、Component、Workflow 与 Trait 边界 |
| `architecture-diagrams.md` | Implemented Reference | 当前四角色、Workflow execution 和 Trait 映射图 |
| `kafka-queue-implementation.md` | Implemented Reference | Kafka 队列实现 |
| `cloudjob-skeleton.md` | Implemented Reference | CloudJob 基础说明 |
| `cloudjob-custom-provider-template.md` | Implemented Reference | Custom CloudJob Provider 扩展 |
| `core-module-boundary-and-cross-layer-contracts.md` | Implemented Reference | 核心模块边界与跨层字段契约 |
| `project-style-guide.md` | Implemented Reference | 项目命名、模块边界与架构风格维护基线 |
| `statefulset-pvc-volume-naming.md` | Implemented Reference | StatefulSet PVC 命名分析 |
| `template-instantiation-from-tem-id.md` | Implemented Reference | 基于模板 ID 的实例化 |
| `template-storage-identity-fix-plan.md` | Implemented Reference | 模板持久化存储 Identity 的当前实现与回归验证 |
| `workflow-task-deduplication-zh.md` | Implemented Reference | 工作流任务去重方案 |

## 测试与审计

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `login-token-authz-analysis-2026-09-04.md` | Historical / Audit | opaque 登录 Token、会话撤销、空间授权与 JWT 必要性评估；记录 refresh 重放检测、空闲超时、清理和路由策略测试的后续处置 |
| `go-idiomatic-code-quality-audit-2026-08-09.md` | Historical / Audit | 基于 `aaec6307` 的全仓 Go 惯用性、接口、并发、错误传播与测试组织审计 |
| `existing-cluster-application-import-analysis-2026-08-03.md` | Historical / Audit | 现有集群应用进入 Eruun 的发现、observe/adopted 导入、source identity、执行边界与上线验收分析 |
| `code-quality-audit-2026-05-20.md` | Historical / Audit | 全仓代码质量审计与后续简化重构建议 |
| `master-hidden-issues-audit-2026-07-10.md` | Historical / Audit | 当时 master 基线的未记录 Bug、潜在问题、严重级别、代码证据与修复建议 |
| `workflow-testing-guide.md` | Historical / Audit | 工作流手工测试指南 |
| `version-update-testing.md` | Historical / Audit | 版本更新测试流程 |
| `master-code-review-open-items.md` | Historical / Audit | 代码评审遗留项 |
| `code-quality-audit-action-map.md` | Current | 当前 Go 质量审计项到小 PR 行动线、验证重点与推进顺序的映射 |
| `设计备案.md` | Historical / Audit | 早期设计备案 |

## 草案与演进方案

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `ai-runtime-vision.md` | Draft / Proposal | AI Runtime 产品方向、能力分层、路线图和实施门禁 |
| `agent-evaluation-job-design.md` | Draft / Proposal | Agent 评测输入、执行、制品、隔离和质量门禁方向 |
| `architecture-refactor-plan.md` | Draft / Proposal | 架构演进方案 |
| `ai-provider-integration-design.md` | Draft / Proposal | 托管模型、对象存储、计算及 AI 云 Provider 集成边界 |
| `design-custom-update-workflow.md` | Draft / Proposal | 自定义更新工作流 |
| `gateway-abstraction-design.md` | Draft / Proposal | Gateway 抽象层草案 |
| `gateway-trait-implementation-plan.md` | Draft / Proposal | Gateway Trait 实施计划 |
| `vectorization-job-design.md` | Draft / Proposal | Provider-neutral 向量化批处理方向 |
| `vllm-hami-distributed-inference-design.md` | Draft / Proposal | 自托管模型服务、GPU、vLLM/HAMi 与多节点 adapter 方向 |
| `workflow-conditional-branching-design.md` | Draft / Proposal | 条件分支设计 |
| `workflow-global-scheduler-design.md` | Draft / Proposal | Workflow 优先级、公平性、配额、容量准入和可抢占方向 |
| `workflow-resource-capacity-scheduler.md` | Draft / Proposal | Workflow 资源容量准入与可选容量补偿边界 |

## 图源与资产

- `workflow.drawio`、`workflow.excalidraw`、`无标题-*.excalidraw`：历史图源文件，保留供后续编辑。

## 文档维护规则

- 新增行为、API、配置、错误契约、默认值或工作流语义时，优先更新已有专题文档；没有合适文档时再新增一个聚焦文档。
- 新增文档后，把它加入本文档对应分组，并在需要时从 `README.md` 或 `README_zh.md` 链接。
- 文档描述事实，不替代码补偿行为；如果代码契约和文档冲突，先以代码为准，再修正文档。
- 示例请求/响应放在 `examples/`，专题设计和契约说明放在 `docs/`。
- 每个 Markdown 文档在一级标题后必须使用本页五种状态之一；索引状态必须与正文一致。
- Proposal 不固定未经实现验证的路由、字段、默认值或厂商机制；如需概念示例，必须明确说明不可直接执行。
