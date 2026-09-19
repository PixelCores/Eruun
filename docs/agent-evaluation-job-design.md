# Eruun LLM 评测任务演进方向

> 状态：Draft / Proposal。`main` 已实现 Harbor `eval` 空间 Job 及其单实例认领、阶段/心跳/进度/终态协议；当前公共 API、默认参数与运行边界以 [空间 Job API](workspace-jobs-api.md)、[Harbor Runner](../runners/harbor/README.md) 和 [Runner 实现参考](agent-evaluation-runner-service-design.md) 为准。本文保留更多目标、Judge、质量门禁和其他 Agent 能力的后续演进。

> 当前契约修订：评测已统一为 `type: job` + `traits.evaluation`，支持独立 Job 与应用 Workflow；model 是被测对象，agent 是 harness，env 使用 ack，框架版本属于执行快照。本文后文的 `eval` 为内部执行器/早期设计用语，不再是公共请求类型；后续被测应用引用、Judge 和质量门禁仍属 Proposal。

> 示例说明：本文中的后续流程块仅是概念伪代码，不可直接执行；已注册类型与接口不得由本文重新定义。

## 1. 与 AI Runtime 的关系

[AI Runtime 愿景](ai-runtime-vision.md) 把评测放在统一空间 Job、权限和制品边界中。Eruun 当前已经提供 `/api/v1/jobs`、`traits.evaluation` 规格、无 AppID 的 WorkspaceID/TaskID 持久化、Harbor 0.22.0 Runner、任务包与结果制品，以及一次性 Kubernetes Job 的状态、取消、超时和数据库执行租约。

`command` 与 `eval` 是同一空间 namespace 中执行的两种 Eruun Job。两者都构建 Kubernetes `batch/v1 Job`，Pod 固定 `restartPolicy: Never`、Job 固定 `backoffLimit: 0`，并复用统一 Workflow/Job 执行链路；`eval` 使用平台固定的 Harbor Runner 镜像，`command` 直接运行用户声明的镜像和命令。类型边界见 [同一命名空间中的 Job 类型](ai-runtime-vision.md#42-同一命名空间中的-job-类型)。[Runner 阶段状态增强](agent-evaluation-runner-service-design.md) 只服务 `eval`，不包装 `command` 镜像，也不新增评测任务实体、Scheduler、消息队列或状态机。

### 1.1 独立评测与应用内评测

评测遵循 [AI Runtime 的任务执行身份与应用归属原则](ai-runtime-vision.md#41-任务执行身份与应用归属)。这里的 Workflow Run 表示一次持久化执行，不表示必须创建新的 Run 实体或持久化 Workflow 定义。

| 场景 | 任务归属与应用关联 | 执行身份 |
| --- | --- | --- |
| 当前独立 Harbor 评测 | 必须有已授权的空间，不需要 AppID、Component 或占位 Application；以 evaluation Trait 声明模型、harness、任务包和运行条件 | 新执行由服务端生成 TaskID |
| 后续独立评测 Eruun 中的应用 | 任务属于提交时确定的空间；需要新增、校验并持久化被测应用及不可变版本引用 | 新执行由服务端生成 TaskID，不以目标 AppID 代替 |
| 当前应用 Workflow 中的模型评测步骤 | 继承应用归属与所在 Workflow 执行上下文，使用该路径原有的 AppID | 复用所在执行的 TaskID，以 Job 身份区分评测步骤 |

独立评测的报告、状态、取消与审计围绕 TaskID 关联；数据集、目标 revision 和 case ID 用于说明测了什么，不能替代任务执行身份。相同输入再次发起一次评测应获得新的 TaskID；同一次执行的恢复和重试沿用 TaskID，并分别受 Job 执行身份与 Workflow Worker ownership fence 约束。

当前独立评测与应用 Workflow 内的模型评测已经共用 `EvaluationTraitSpec`。应用内执行并不意味着被测对象就是该应用：被测 Eruun 应用及版本引用仍需后续目标契约，不应与承载评测的 AppID 混淆。

## 2. 目标与非目标

目标：

- 对一个版本化的 Agent 或模型目标运行可重复的数据集。
- 同时采集确定性质量指标、性能、用量、错误和可选 Judge 结果。
- 保存查询摘要，并把逐 case 结果、trace、报告和 checkpoint 作为受控制品管理。
- 允许把 verdict 独立于执行成功/失败展示；只有显式质量门禁才影响 Workflow 结果。
- 继承 Eruun 的 workspace 授权、任务 ownership、取消、超时和审计边界。

非目标：

- 在第一版提供训练、微调、在线 Agent 网关或模型托管。
- 把浏览器/桌面交互 benchmark、跨集群 MapReduce 和第三方评测 SaaS 同时纳入首个实现。
- 宣称对外部模型调用 exactly-once。
- 在文档阶段固定路由、表结构、镜像、并发、超时或抢占次数。

## 3. 最小评测输入

当前 Harbor 实现已经定义的输入见 [空间 Job API](workspace-jobs-api.md)。后续扩展仍应保持最小闭环：

- 评测继续使用 `type: job` 与 `traits.evaluation`；普通一次性命令继续使用 `command`，不增加语义重复的 `custom`。
- 经服务端校验的 workspace 归属和调用者身份；不把 ProjectID、AppID 或 Component 作为所有评测的通用必填信息。
- 不可变的目标引用；它可以是部署后的 Agent、模型端点或后续定义的运行配置。
- 带版本或内容摘要的数据集引用。
- 评分器集合、可选 Judge 引用和阈值策略。
- 资源、并发、重试、预算和截止时间，但默认值由实现与负载测试决定。
- 制品保留和敏感级别。

凭据只允许通过 Secret、短期 Token 或 Provider 管理的引用获得，不接受把明文 API key 写入任务 spec、数据集或日志。

TaskID 属于服务端生成的执行元数据，不是调用方需要预先填写的评测输入。引用 Eruun 应用时，AppID 只能定位应用，仍须固定被测版本或配置摘要；外部端点无法固定模型版本时，应记录可获得的版本证据与可重复性限制，不能仅凭 URL 声称目标不可变。

## 4. 数据集与可重复性

数据集格式至少要表达稳定 case ID、输入、期望值或评分元数据。实现前必须决定：

- 支持哪些来源和格式，以及内容如何得到稳定摘要。
- 如何用 case ID、数据集 revision 和目标 revision 匹配逐 case 结果与 checkpoint，并绑定所属 TaskID 和 Job，避免将相同输入的不同评测混为一次执行。
- 重试后哪些 case 可以跳过，哪些外部请求可能重复。
- 数据集、提示、模型响应和 Judge 输入中哪些内容可以写入日志或制品。

数据集解析和下载属于 Runner 或数据准备步骤，不应进入 Scheduler。格式扩展通过版本化 schema 演进，不用多个任务实体分别表示不同数据源。

## 5. 执行与状态

当前路径复用统一任务提交与执行链路，由一个 WorkflowQueue 任务驱动所属空间 namespace 中的 `batch/v1 Job`，并在现有 Runner 与内部 HTTP 边界报告阶段状态：

```text
submit eval Job
  -> authorize workspace and resolve its namespace
  -> validate Harbor input, dataset, credentials and result policy
  -> allocate TaskID and persist WorkflowQueue/Job intent
  -> establish execution generation and Worker ownership
  -> build batch/v1 Job with the fixed Harbor Runner image
  -> Runner claims the current attempt
  -> Runner downloads the dataset and supervises harbor run
  -> Runner publishes phase/heartbeat/progress events
  -> Runner uploads the final native result archive
  -> Runner publishes terminal referencing the persisted artifact
  -> Kubernetes Job exits and Eruun exposes status/results
```

图中的 claim、阶段、心跳、进度、结果和 terminal 闭环均已实现；本文后续提出的更多目标与质量能力仍是 Proposal。

API 已在接受事务中持久化 WorkspaceID、服务端生成的 TaskID、版本化 JobSpec 和任务绑定能力；Worker 随后建立 ExecutionKey、Job RunGeneration 与 Attempt，并创建与 TaskID 绑定的 Kubernetes Job。阶段事件应复用这些身份及现有 Pod 名称/UID 校验，不创建 RunnerTask、claim 表或平行状态机。为覆盖 Kubernetes 在 Pod terminating 等窗口创建 replacement Pod 的可能性，Runner 必须在启动 Harbor 前通过同一内部状态协议，以 CAS 把当前 execution identity/attempt 原子认领给一个 Pod UID；认领 checkpoint 保存在现有 JobInfo/InternalInfo 事务边界中。

`command` 走同一空间 Job 链路，但直接运行用户声明的镜像和命令，以进程退出及 Kubernetes Job 状态收敛，不进入 Harbor 评分、阶段状态或原始评测结果上传协议。分别提交的两类任务各自获得 TaskID；类型只说明 Job 做什么，不决定空间归属，也不替代执行身份。

Agent 评测不能把 Pod `Running` 当作业务完成信号。当前 `restartPolicy: Never` 禁止 kubelet 重启已退出容器，`backoffLimit: 0` 禁止 Job 在已计入失败后继续重试，但两者不能单独保证 terminating/deletion 窗口内只有一个 Pod 执行业务。Runner 通过 CAS 成功认领当前 execution identity/attempt 后才能启动 Harbor；同一 Pod 的重复认领幂等，其他 Pod 必须退出且不得产生评测副作用。阶段协议应让已认领 Runner 上报 preparing/running/finalizing 等有界阶段，并以独立 terminal 事件提交 succeeded/failed 证据；Runner 整体 OOM、崩溃或节点丢失而无法上报时，Eruun 继续使用当前 Job/Pod 终止状态和事件兜底。显式重试必须建立新的 execution identity/attempt，迟到事件不能覆盖当前执行。

Runner 上报必须匹配持久化的 `JobInfo` 执行身份、attempt、Pod UID 和所属 live Job UID；Workflow Worker ownership 换代不应使仍被恢复的已提交 Job 执行失效。Worker 推进状态和清理资源则必须继续通过当前 `WorkflowQueue` generation/token/worker ownership fence。被新 Job 执行或 attempt 取代的迟到进度和报告不能覆盖当前执行。网络不确定时，单个 case 的模型请求可能重复，报告需要能够标记这种不确定性。

## 6. 评分、指标和 verdict

最低指标集合应覆盖：

- case 总数、完成数、跳过数和错误数。
- 确定性 scorer 的通过率与分项得分。
- 请求延迟、吞吐、超时、重试和外部错误。
- 可获得时的输入/输出 Token 或其他用量。
- Judge 的版本、输入摘要、得分与错误，且与确定性分数分开展示。

`execution status` 与 `quality verdict` 必须是不同概念：执行可以成功完成但未达到质量阈值。只有调用方显式启用 quality gate 时，threshold miss 才能使所属 Workflow 失败。

## 7. 制品与查询

数据库适合保存任务归属、状态、进度、摘要、verdict 和制品清单；逐 case 响应、trace、checkpoint 和大型报告应放在受控对象存储或等价 ArtifactStore 中。

查询能力必须做到：

- 调用者重新通过任务所属 workspace 授权，不能只凭 TaskID 或被测 AppID 读取；独立任务不依赖目标应用存在才能确定报告归属。
- 返回短期授权链接或受控流，不暴露永久对象存储凭据。
- 日志、trace、指标 label 和错误消息不包含请求正文、模型响应或 Secret，除非数据策略明确允许。
- 删除、保留和导出策略覆盖数据库摘要与外部制品。

## 8. 隔离和权限

- `command` 与 `eval` 的 Kubernetes Job 在同一空间 namespace 中执行，各自绑定 TaskID；共用 namespace 不表示可以访问其他 Job 的凭据或制品。
- `command` 默认不挂载 Kubernetes API Token。Harbor Runner 因需要创建、exec、观察和删除 trial Pods，使用平台管理的 namespace 级专用 ServiceAccount；当前 Role 的 Pod 与 pods/exec 权限作用于整个 namespace，不能描述成由 RBAC 强制限定为 trial Pods。trial Pod 使用独立的无权限 ServiceAccount。后续若要求平台强制“只能操作本任务 trial Pods”，必须增加可验证的 admission、代理或隔离边界。
- 出站网络只允许目标端点、数据源、ArtifactStore 和必要授权端点。
- Judge 和被测目标使用彼此独立的凭据引用。
- 取消和超时要终止运行负载、停止新请求并收敛可恢复的进度；不得因清理失败删除其他任务制品。
- 独立评测只拥有本次执行的临时资源。取消评测不删除被测应用或数据源；目标被删除、版本失效或授权撤销时须明确终止或失败收敛，不能改测另一个版本。应用内评测则继续受所属 Workflow 的取消与清理规则约束。

## 9. 抢占与 checkpoint

抢占不是首个版本的前置条件。只有在真实排队压力证明需要、Runner 已能生成可验证 checkpoint、重复请求影响可接受，并且 Scheduler 能安全释放容量后，才设计协作式抢占。

checkpoint 至少需要绑定任务、数据集、目标、Runner 版本和已完成 case 游标。具体控制接口、等待时间和重试次数留给实现 PR，不在本 Proposal 固定。

## 10. 已实现的单实例认领与阶段状态基线

1. 在现有 Harbor Runner 上增加有界 phase/heartbeat/terminal 事件，不改变 `eval` 的公共提交与结果 API。
2. 复用当前任务能力、Pod 名称/UID、ExecutionKey、RunGeneration 和 Attempt 完成认证与迟到写入隔离；在现有 JobInfo/InternalInfo 中增加单实例 CAS 认领，不新增 claim 表或顶层实体。
3. 验证子进程失败时 Runner 能先上传诊断和终态再退出，Runner 整体 OOM 时由 Kubernetes 证据兜底。
4. 保持现有完整原始结果、ArtifactStore、空间授权、取消、超时和保留策略不退化。
5. Judge、质量门禁、更细 checkpoint 和抢占仍只在真实需求和资源数据支持后另行设计。

单实例认领与阶段状态基线须持续覆盖以下验收场景；其中生产环境和未来能力仍以实际证据为准：

| 场景 | 必须验证的结果 |
| --- | --- |
| 同一 namespace 中运行两类 Job | `command` 与 `eval` 各自创建 `batch/v1 Job`，身份绑定 TaskID 且不会碰撞或交叉清理；`command` 不被注入评测协议 |
| Agent 评测状态与完成 | Pod Running 不作为评测完成；阶段和终态上报绑定当前执行与 Pod 身份；最终归档、进程退出和 Kubernetes Job 状态按明确顺序收敛 |
| 自动防重 | `restartPolicy: Never`、`backoffLimit: 0` 保持有效；同一 execution identity/attempt 只允许一个 Pod UID 认领成功，replacement Pod 在启动 Harbor 前被拒绝；显式新 attempt 才能重新执行 |
| 类型校验与恢复 | 类型缺失、未知或无权使用时明确拒绝；已接受 Job 的类型在持久化、执行、状态查询和恢复中保持一致，不退化为默认类型 |
| 无 AppID 的独立提交 | 经空间和输入授权后生成 TaskID；不创建占位 Application、Component 或 Workflow 定义；持久化失败不返回接受结果 |
| 空间归属缺失或跨空间访问 | 提交、执行、查询、取消及制品访问拒绝未授权操作；不能凭 TaskID 或目标 AppID 绕过 |
| 相同输入再次运行与故障恢复 | 新运行产生新 TaskID；恢复沿用原 TaskID；Worker ownership 换代可继续观察原已提交 Job 执行；被新 Job 执行或 attempt 取代的迟到结果不能覆盖当前结果 |
| 多个 Job 或未来应用内评测 | 独立提交各自生成 TaskID；未来应用内多个 Job 共享所属执行 TaskID 时明细仍可区分，应用工作流原有 AppID 校验和生命周期保持有效 |
| 无应用任务的后台执行 | 空间调度配额、取消、超时、状态/回调和清理均能从持久化任务确定归属，不依赖伪造 AppID |
| 提交与空间删除并发 | 不在已删除空间中接受任务；未完成任务继续约束空间删除；制品清理与保留规则有验证证据 |
| 目标失效与清理 | 引用应用被删除或权限撤销时明确收敛；不改测其他版本、不误删目标资源；已产生结果按原空间权限和保留策略处理 |

claim/phase/heartbeat/progress/terminal 的可执行契约已进入 Current 文档。下表同时保留未来扩展必须继续满足的回归边界；未完成的生产集群故障矩阵仍是 Draft PR 转 Ready 的交付门禁，不改变协议已经实现的事实。
