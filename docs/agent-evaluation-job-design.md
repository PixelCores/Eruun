# Eruun Agent 评测任务方向

> 状态：Draft / Proposal。本文描述评测能力应解决的问题和实施门禁，不定义已承诺的 API、数据库字段、任务类型、Runner 协议或默认参数。

> 示例说明：本文中的流程块仅是概念伪代码，不可直接执行，也不代表已注册的类型或接口。

## 1. 与 AI Runtime 的关系

[AI Runtime 愿景](ai-runtime-vision.md) 把评测放在 Kubernetes 自托管 Agent 与权限边界之后。Eruun 当前有 Application Workflow、一次性 Kubernetes Job、任务状态、日志、取消、超时和数据库执行租约，但没有 Agent evaluation 专用路由、领域模型或 Runner。

评测能力应优先复用统一 Workflow 执行链路，不因为需要报告和指标就复制一套 Scheduler、消息队列或任务状态机。是否需要独立公共入口或内部任务类型，由最小实现验证后决定。

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

- workspace/project 归属和调用者身份。
- 不可变的目标引用；它可以是部署后的 Agent、模型端点或后续定义的运行配置。
- 带版本或内容摘要的数据集引用。
- 评分器集合、可选 Judge 引用和阈值策略。
- 资源、并发、重试、预算和截止时间，但默认值由实现与负载测试决定。
- 制品保留和敏感级别。

凭据只允许通过 Secret、短期 Token 或 Provider 管理的引用获得，不接受把明文 API key 写入任务 spec、数据集或日志。

## 4. 数据集与可重复性

数据集格式至少要表达稳定 case ID、输入、期望值或评分元数据。实现前必须决定：

- 支持哪些来源和格式，以及内容如何得到稳定摘要。
- case ID、数据集 revision 和目标 revision 如何共同形成执行身份。
- 重试后哪些 case 可以跳过，哪些外部请求可能重复。
- 数据集、提示、模型响应和 Judge 输入中哪些内容可以写入日志或制品。

数据集解析和下载属于 Runner 或数据准备步骤，不应进入 Scheduler。格式扩展通过版本化 schema 演进，不用多个任务实体分别表示不同数据源。

## 5. 执行与状态

推荐的最小路径是一个 Workflow Run 驱动一个隔离的 Kubernetes Job：

```text
submit evaluation intent
  -> persist workflow-owned task
  -> run isolated evaluation workload
  -> publish progress and artifacts
  -> calculate verdict
  -> complete workflow and expose summary
```

该图是方向说明，不代表已经存在对应路由或 JobType。

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

- 调用者重新通过 workspace/project 授权，不能只凭 task ID 读取。
- 返回短期授权链接或受控流，不暴露永久对象存储凭据。
- 日志、trace、指标 label 和错误消息不包含请求正文、模型响应或 Secret，除非数据策略明确允许。
- 删除、保留和导出策略覆盖数据库摘要与外部制品。

## 8. 隔离和权限

- 评测 Pod 使用任务作用域身份，不复用 Eruun 控制面 ServiceAccount。
- 默认不挂载 Kubernetes API Token；确有集群 API 需求时使用最小 RBAC。
- 出站网络只允许目标端点、数据源、ArtifactStore 和必要授权端点。
- Judge 和被测目标使用彼此独立的凭据引用。
- 取消和超时要终止运行负载、停止新请求并收敛可恢复的进度；不得因清理失败删除其他任务制品。

## 9. 抢占与 checkpoint

抢占不是首个版本的前置条件。只有在真实排队压力证明需要、Runner 已能生成可验证 checkpoint、重复请求影响可接受，并且 Scheduler 能安全释放容量后，才设计协作式抢占。

checkpoint 至少需要绑定任务、数据集、目标、Runner 版本和已完成 case 游标。具体控制接口、等待时间和重试次数留给实现 PR，不在本 Proposal 固定。

## 10. 实施门禁

1. 先完成一个固定目标、固定数据集、确定性 scorer 的端到端实验。
2. 再加入受控制品、权限校验和可观察进度。
3. 根据实验决定是否需要专用 API/任务类型，而不是预先创建新实体。
4. 增加 Judge、并发和质量门禁，并验证预算与失败语义。
5. 只有出现可度量的资源争用后才评估优先级、配额和抢占。

升级为 Current 需要代码、迁移（如有）、API/DTO、执行器、权限、测试、部署和运维证据完整闭环。
