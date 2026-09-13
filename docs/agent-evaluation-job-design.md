# Eruun Agent 评测任务方向

> 状态：Draft / Proposal。本文描述评测能力应解决的问题和实施门禁，不定义已承诺的 API、数据库字段、任务类型、Runner 协议或默认参数。

> 示例说明：本文中的流程块仅是概念伪代码，不可直接执行，也不代表已注册的类型或接口。

## 1. 与 AI Runtime 的关系

[AI Runtime 愿景](ai-runtime-vision.md) 把评测放在 Kubernetes 自托管 Agent 与权限边界之后。Eruun 当前有 Application Workflow、Deployment 与一次性 Kubernetes Job、任务状态、日志、取消、超时和数据库执行租约，但没有 Agent evaluation 专用路由、领域模型或 Runner。

Agent 评测任务与用户自定义任务是同一空间 namespace 中执行的不同 Eruun Job，通过一个 Job 类型区分。两者都由相应控制器按类型和输入选择指定执行镜像并创建 Kubernetes Deployment，由该镜像创建并运行一次性任务；它们不使用 Kubernetes `batch/v1 Job`。两者复用统一 Workflow/Job 执行链路，评测所需的输入、Runner 配置、指标和报告由该类型的处理逻辑负责。类型的共用规则见 [同一命名空间中的 Job 类型](ai-runtime-vision.md#42-同一命名空间中的-job-类型)；本草案不新增独立的评测任务实体、Scheduler、消息队列或状态机。

### 1.1 独立评测与应用内评测

评测遵循 [AI Runtime 的任务执行身份与应用归属原则](ai-runtime-vision.md#41-任务执行身份与应用归属)。这里的 Workflow Run 表示一次持久化执行，不表示必须创建新的 Run 实体或持久化 Workflow 定义。

| 场景 | 任务归属与应用关联 | 执行身份 |
| --- | --- | --- |
| 独立评测外部 Agent 或模型端点 | 必须有已授权的空间，不需要 AppID、Component 或占位 Application；端点与凭据仍需单独授权 | 新执行由服务端生成 TaskID |
| 独立评测 Eruun 中的应用 | 任务属于提交时确定的空间，以目标引用记录被测应用及版本；引用不自动变成应用所有权 | 新执行由服务端生成 TaskID，不以目标 AppID 代替 |
| 应用部署 Workflow 中的评测步骤 | 继承应用归属与所在 Workflow 执行上下文，使用该路径原有的 AppID | 复用所在执行的 TaskID，以 Job 身份区分评测步骤 |

独立评测的报告、状态、取消与审计围绕 TaskID 关联；数据集、目标 revision 和 case ID 用于说明测了什么，不能替代任务执行身份。相同输入再次发起一次评测应获得新的 TaskID；同一次执行的恢复和重试沿用 TaskID，并受当前执行代约束。

这些场景是目标设计。当前无应用任务的实现先例是资源导入扫描与纳管，评测仍需补齐独立输入、执行构建、授权、调度和结果协议，不能直接将现有应用执行请求中的 AppID 留空来调用。

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

首个实现应只覆盖能够形成闭环的输入：

- Job 类型选择 Agent 评测；具体枚举名称由实现确定。用户自定义 Job 使用同一模型的另一类型，不要求填写评测专用的目标、数据集或评分字段。
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

推荐的最小路径是复用统一任务提交与执行链路，由一个 Workflow Run 驱动所属空间 namespace 中的任务执行 Deployment，类型决定评测输入、执行镜像与结果处理：

```text
submit Job intent with an evaluation type
  -> authorize workspace and resolve its namespace
  -> validate Job type and evaluation inputs
  -> allocate TaskID for a new standalone execution
  -> persist workflow-owned task and typed Job execution
  -> create a Deployment with the selected execution image
  -> let the image create and run the one-off evaluation task
  -> publish progress and artifacts
  -> calculate verdict
  -> complete workflow and expose summary
```

该图是方向说明，不代表已经存在对应路由或 JobType。

图中分配 TaskID 的步骤面向新提交的独立评测；作为应用 Workflow 中的步骤运行时，评测复用已存在的 TaskID。任务持久化成功后才能返回接受结果并进入调度。单个评测 Job 和后续按需拆分的数据准备、执行、报告 Job 都应复用这一任务身份，不建立第二套评测状态机；每个需要独立运行载体的 Job 创建并管理自己的 Deployment。

用户自定义 Job 走同一执行链路，按其类型校验镜像、命令和输入输出，选择相应执行镜像并创建 Deployment，不进入评测专用的评分流程。同一 namespace 中分别提交的评测和自定义任务各自获得 TaskID；若被编排在同一次 Workflow 执行中，则共享 TaskID 并以 Job 身份区分。类型只说明 Job 做什么，不决定任务归属，也不改变命名空间或替代执行身份。

Deployment Ready 不能作为一次性任务完成信号。执行镜像必须通过实现时确定的结果协议上报进度和终态，且上报需绑定当前 TaskID、Job 身份和执行代；控制器在任务成功、失败、取消或超时收敛后停止并清理对应 Deployment。若镜像主进程随任务完成而退出，还必须避免 Deployment 自动重启容器并重复执行任务。

执行必须遵循现有 generation/token fencing。Runner 上报只能影响当前执行代；旧执行的迟到进度和报告不能覆盖新执行。网络不确定时，单个 case 的模型请求可能重复，报告需要能够标记这种不确定性。

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

- 评测和自定义 Job 的 Deployment 在同一空间 namespace 中执行，各自使用任务作用域身份，不复用 Eruun 控制面 ServiceAccount；共用 namespace 不表示可以访问其他 Job 的凭据或制品。
- 默认不挂载 Kubernetes API Token；确有集群 API 需求时使用最小 RBAC。
- 出站网络只允许目标端点、数据源、ArtifactStore 和必要授权端点。
- Judge 和被测目标使用彼此独立的凭据引用。
- 取消和超时要终止运行负载、停止新请求并收敛可恢复的进度；不得因清理失败删除其他任务制品。
- 独立评测只拥有本次执行的临时资源。取消评测不删除被测应用或数据源；目标被删除、版本失效或授权撤销时须明确终止或失败收敛，不能改测另一个版本。应用内评测则继续受所属 Workflow 的取消与清理规则约束。

## 9. 抢占与 checkpoint

抢占不是首个版本的前置条件。只有在真实排队压力证明需要、Runner 已能生成可验证 checkpoint、重复请求影响可接受，并且 Scheduler 能安全释放容量后，才设计协作式抢占。

checkpoint 至少需要绑定任务、数据集、目标、Runner 版本和已完成 case 游标。具体控制接口、等待时间和重试次数留给实现 PR，不在本 Proposal 固定。

## 10. 实施门禁

1. 先在同一空间 namespace 中通过各自的 Deployment 运行一个用户自定义 Job 和一个固定目标、固定数据集、确定性 scorer 的评测 Job，验证统一模型与类型分发、无需创建 Application、服务端生成 TaskID 并持久化空间归属。
2. 再加入受控制品、权限校验和可观察进度。
3. 根据实验确定单一 Job 类型的枚举与输入映射，补齐现有类型分发、调度、恢复和清理路径，不增加平行任务实体。
4. 增加 Judge、并发和质量门禁，并验证预算与失败语义。
5. 只有出现可度量的资源争用后才评估优先级、配额和抢占。

最小实现还须覆盖以下验收场景；它们是后续实现要求，不表示当前已通过：

| 场景 | 必须验证的结果 |
| --- | --- |
| 同一 namespace 中运行两类 Job | 一个类型字段区分评测与自定义任务；各自以指定执行镜像创建 Deployment；Deployment 身份绑定 TaskID 与 Job 且不会碰撞或交叉清理；复用调度与生命周期；自定义任务不要求评测专用字段；不按类型创建新 namespace |
| Deployment 完成与清理 | Ready 不作为任务完成；结果上报绑定 TaskID、Job 身份与执行代；成功、失败、取消和超时后停止并清理对应 Deployment；执行镜像退出时不产生重复执行 |
| 类型校验与恢复 | 类型缺失、未知或无权使用时明确拒绝；已接受 Job 的类型在持久化、执行、状态查询和恢复中保持一致，不退化为默认类型 |
| 无 AppID 的独立提交 | 经空间和输入授权后生成 TaskID；不创建占位 Application、Component 或 Workflow 定义；持久化失败不返回接受结果 |
| 空间归属缺失或跨空间访问 | 提交、执行、查询、取消及制品访问拒绝未授权操作；不能凭 TaskID 或目标 AppID 绕过 |
| 相同输入再次运行与故障恢复 | 新运行产生新 TaskID；恢复沿用原 TaskID；旧执行代的迟到结果不能覆盖当前结果 |
| 多个 Job 或应用内评测 | 多个 Job 共享所属执行的 TaskID，明细可区分；应用工作流原有 AppID 校验和生命周期保持有效 |
| 无应用任务的后台执行 | 空间调度配额、取消、超时、状态/回调和清理均能从持久化任务确定归属，不依赖伪造 AppID |
| 提交与空间删除并发 | 不在已删除空间中接受任务；未完成任务继续约束空间删除；制品清理与保留规则有验证证据 |
| 目标失效与清理 | 引用应用被删除或权限撤销时明确收敛；不改测其他版本、不误删目标资源；已产生结果按原空间权限和保留策略处理 |

升级为 Current 需要代码、迁移（如有）、API/DTO、执行器、权限、测试、部署和运维证据完整闭环。
