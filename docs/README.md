# Eruun 文档索引

> 状态：Current。本文是 `docs/` 的导航入口，按当前 `master` 代码事实整理文档状态，并为维护者和 LLM 提供需求定位路径。

## 推荐阅读顺序

1. `AGENTS.md`：仓库协作规则、构建测试命令、提交和 PR 要求。
2. 本文档：目录分层、需求定位、当前代码事实和文档索引。
3. `core-module-boundary-and-cross-layer-contracts.md`：API、Domain、DB、Cache、K8s 的核心字段契约。
4. `workflow-architecture-guide.md`：工作流调度、队列、Job 执行和状态机。
5. `enterprise-distributed-runtime-design.md`：四类运行角色、双 Leader、Workflow 数据库租约和部署拓扑。
6. `architecture-diagrams.md`：架构图、DDD 分层图、消息队列、Informer、Traits 和目录结构。

## 状态约定

- `Current`：面向用户的当前可用能力说明，应与代码和命令输出保持一致。
- `Implemented Reference`：已实现能力的设计/行为参考，可能比快速入门更细。
- `Draft / Proposal`：设计草案或后续演进方案，不代表当前已暴露能力。
- `Historical / Audit`：历史备案、审计或测试记录，用于追溯，不作为最新使用手册。
- `Deprecated`：兼容或迁移参考，不作为新接入、新功能或当前主路径的依据。

## LLM / CONTEXT / ADR 使用规则

- `Implemented Reference` 可用于理解实现背景和内部结构，但写入 CONTEXT、ADR 或对外说明前，必须用代码、命令输出或 `Current` 专题文档二次确认。
- `Draft / Proposal`、`Historical / Audit`、`Deprecated` 只能作为设计背景、迁移线索或审计证据，不得改写为当前已暴露能力。
- `devlogs/` 记录一次 PR/主题的当时决策，可能被后续 devlog 或 `Current` 文档 supersede；当前事实以代码与 `docs/` 中的 `Current` 文档为准。

## 当前代码事实速查

- API 前缀：`/api/v1`。
- 服务进程默认监听：`127.0.0.1:8000`；`deploy/eruun-stack.yaml` 会通过 `ERUUN_BIND_ADDR=0.0.0.0:8000` 暴露集群内服务。
- 服务端角色通过 `--role` / `ERUUN_ROLE` 显式选择 `api/controller/scheduler/worker/all`；默认 `all` 在单进程内运行全部职责。
- Workflow 固定使用 v2 generation/token ownership 与数据库执行租约；不存在关闭 fencing 或处理 v1 dispatch 的运行模式。
- 顶层 `/workflow`、`/workflow/exec`、`/workflow/cancel` 路由不再注册；应用维度 workflow API 是当前主路径。
- OAuth 登录路由与 `/authz/*` 管理路由当前未注册；`apiAuth` 中间件已接入全局路由，但默认配置为 `enabled=false`。

## 目录层级速查

| 路径 | 职责 | 常见需求入口 | 注意事项 |
| --- | --- | --- | --- |
| `cmd/main.go`, `cmd/server/app` | API Server 启动、参数、服务装配 | 新增启动参数、调整初始化顺序 | 配置问题优先 fail-fast，不要静默降级 |
| `pkg/apiserver/interfaces/api` | HTTP 路由、参数绑定、响应封装、中间件 | 新接口、接口校验、认证授权、流式能力 | 不直接写 DB/K8s，业务逻辑下沉到 Domain |
| `pkg/apiserver/interfaces/api/dto/v1` | API DTO 与请求/响应结构 | 字段增删、响应形态调整 | 同步 assembler、文档和 examples |
| `pkg/apiserver/interfaces/api/assembler/v1` | Domain 对象到 DTO 的组装 | 响应字段推导、脱敏、兼容字段 | 不放持久化或 K8s 调用逻辑 |
| `pkg/apiserver/domain/model` | GORM 模型和领域实体 | 新表字段、状态字段、业务实体 | 字段语义必须同步跨层契约文档 |
| `pkg/apiserver/domain/repository` | 仓储接口和数据访问契约 | 查询/写入方法、事务边界 | 接口表达业务意图，不暴露上层 DTO |
| `pkg/apiserver/domain/service` | 应用生命周期、转换、查询、工作流创建 | 创建/更新/删除应用、组件查询、K8s YAML 转换 | 保持业务规则集中，避免依赖接口层细节 |
| `pkg/apiserver/domain/spec` | 配置规格、策略和校验 | Auth、OAuth、URL 安全、云资源配置校验 | 校验规则要有失败路径测试 |
| `pkg/apiserver/event/workflow` | Workflow 调度、分发、状态推进、审批/超时 | 任务状态、队列消费、分布式执行 | DB 状态机是任务事实源 |
| `pkg/apiserver/event/workflow/job` | 具体 Job 控制器和 K8s 资源调和 | Deployment、StatefulSet、Service、PVC、Secret、RBAC 等资源执行 | 保持资源生成、等待和清理语义一致 |
| `pkg/apiserver/event/workflow/cloudjob` | 云资源 Provider 合约与实现 | 云资源步骤、Provider 注册、外部云动作 | 合约字符串集中为常量 |
| `pkg/apiserver/workflow/traits` | OAM Traits 处理器 | storage、env、probe、resources、sidecar、rbac、ingress 等 Trait | 新 Trait 需要处理顺序、测试和文档 |
| `pkg/apiserver/workflow/naming` | 资源命名规则 | Kubernetes 资源名、PVC/Service 命名 | 命名变化影响状态同步和清理 |
| `pkg/apiserver/infrastructure` | 外部系统适配 | K8s、Redis、Kafka、MySQL、Informer、锁、可观测性 | Infrastructure 实现接口，不反向承载业务规则 |
| `pkg/apiserver/utils` | 通用工具 | 缓存、错误码、异步执行、K8s helper、profiling | 新工具必须可复用，避免放业务分支 |
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
| 认证、授权、OAuth | `pkg/apiserver/interfaces/api/auth` | auth provider、middleware、domain spec | `api-auth-authz-foundation.md`, `oauth2-google-login.md` |
| 配置、系统设置、安全策略 | `pkg/apiserver/config`, `pkg/apiserver/domain/spec` | config defaults、validation、system setting service | `system-setting.md`, `url-security-policy.md` |
| 消息队列或分布式执行 | `pkg/apiserver/infrastructure/messaging`, `pkg/apiserver/domain/repository/workflow_lease.go` | 运行角色、Redis Streams、Kafka、workflow worker、DB lease/fencing | `enterprise-distributed-runtime-design.md`, `leader-informer-recovery.md`, `kafka-queue-implementation.md`, `workflow-architecture-guide.md` |

## 当前能力入口

| 文档 | 状态 | 用途 |
| --- | --- | --- |
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
| `helm-deployment.md` | Current | Helm 四角色拓扑、探针、PDB、topology spread、ServiceAccount 与 RBAC 契约 |

## 实现参考

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `workflow-architecture-guide.md` | Implemented Reference | 工作流引擎架构详解 |
| `enterprise-distributed-runtime-design.md` | Implemented Reference | 分布式运行时：角色、Leader Election、数据库 lease/fencing、Cron 有界分页与失败计划重试、Worker observer 和 Helm 拓扑 |
| `架构文档.md` | Implemented Reference | 架构与 OAM Traits 汇总 |
| `architecture-diagrams.md` | Implemented Reference | 架构图与流程图 |
| `kafka-queue-implementation.md` | Implemented Reference | Kafka 队列实现 |
| `cloudjob-skeleton.md` | Implemented Reference | CloudJob 基础说明 |
| `cloudjob-custom-provider-template.md` | Implemented Reference | Custom CloudJob Provider 扩展 |
| `api-auth-authz-foundation.md` | Implemented Reference | API 鉴权/授权一期 |
| `oauth2-google-login.md` | Implemented Reference | Google OAuth2 后端流程，当前路由未暴露 |
| `oauth2-google-login-integration.md` | Implemented Reference | Google OAuth2 前后端接入，当前路由未暴露 |
| `core-module-boundary-and-cross-layer-contracts.md` | Implemented Reference | 核心模块边界与跨层字段契约 |
| `project-style-guide.md` | Implemented Reference | 项目命名、模块边界与架构风格维护基线 |
| `devlogs/2026-08-03-explicit-namespace-adoption-api.md` | Implemented Reference | 显式 adopted import/cleanup API 激活决策与安全边界 |
| `statefulset-pvc-volume-naming.md` | Implemented Reference | StatefulSet PVC 命名分析 |
| `template-instantiation-from-tem-id.md` | Implemented Reference | 基于模板 ID 的实例化 |
| `workflow-task-deduplication-zh.md` | Implemented Reference | 工作流任务去重方案 |

## 测试与审计

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `go-idiomatic-code-quality-audit-2026-08-09.md` | Historical / Audit | 基于 `aaec6307` 的全仓 Go 惯用性、接口、并发、错误传播与测试组织审计 |
| `existing-cluster-application-import-analysis-2026-08-03.md` | Historical / Audit | 现有集群应用进入 Eruun 的发现、observe/adopted 导入、source identity、执行边界与上线验收分析 |
| `code-quality-audit-2026-05-20.md` | Historical / Audit | 全仓代码质量审计与后续简化重构建议 |
| `master-hidden-issues-audit-2026-07-10.md` | Historical / Audit | 当前 master 未记录 Bug、潜在问题、严重级别、代码证据与修复建议 |
| `workflow-testing-guide.md` | Historical / Audit | 工作流手工测试指南 |
| `version-update-testing.md` | Historical / Audit | 版本更新测试流程 |
| `master-code-review-open-items.md` | Historical / Audit | 代码评审遗留项 |
| `code-quality-audit-action-map.md` | Current | 当前 Go 质量审计项到小 PR 行动线、验证重点与推进顺序的映射 |
| `设计备案.md` | Historical / Audit | 早期设计备案 |

## 草案与演进方案

| 文档 | 状态 | 用途 |
| --- | --- | --- |
| `agent-evaluation-job-design.md` | Draft / Proposal | Agent evaluation Job 的执行、隔离与结果契约 |
| `architecture-refactor-plan.md` | Draft / Proposal | 架构演进方案 |
| `cloudjob-hotplug-provider-design.md` | Draft / Proposal | CloudJob 热插拔 Provider |
| `design-custom-update-workflow.md` | Draft / Proposal | 自定义更新工作流 |
| `gateway-abstraction-design.md` | Draft / Proposal | Gateway 抽象层草案 |
| `gateway-trait-implementation-plan.md` | Draft / Proposal | Gateway Trait 实施计划 |
| `template-storage-identity-fix-plan.md` | Draft / Proposal | 模板同名 persistent storage 复用与重复 PVC 修复执行方案 |
| `vectorization-job-design.md` | Draft / Proposal | 向量化 Job 设计 |
| `vllm-hami-distributed-inference-design.md` | Draft / Proposal | vLLM + HAMi 单节点与跨节点多 GPU 推理工作流设计 |
| `workflow-conditional-branching-design.md` | Draft / Proposal | 条件分支设计 |
| `workflow-global-scheduler-design.md` | Draft / Proposal | Workflow 全局调度器与跨 Worker 协调设计 |
| `workflow-resource-capacity-scheduler.md` | Draft / Proposal | 资源容量调度器设计 |

## 图源与资产

- `workflow.drawio`、`workflow.excalidraw`、`无标题-*.excalidraw`：历史图源文件，保留供后续编辑。

## 文档维护规则

- 新增行为、API、配置、错误契约、默认值或工作流语义时，优先更新已有专题文档；没有合适文档时再新增一个聚焦文档。
- 新增文档后，把它加入本文档对应分组，并在需要时从 `README.md` 或 `README_zh.md` 链接。
- 文档描述事实，不替代码补偿行为；如果代码契约和文档冲突，先以代码为准，再修正文档。
- 示例请求/响应放在 `examples/`，专题设计和契约说明放在 `docs/`。
