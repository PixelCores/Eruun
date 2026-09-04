# Eruun AI Provider 集成方向

> 状态：Draft / Proposal。本文描述托管模型、对象存储、计算和其他 AI 云能力的集成边界，不代表当前支持运行时插件加载、Provider 热更新、通用模型 API 或多云编排。

## 1. 当前事实

当前 `cloudjob` 是 Application Component 类型，使用 `properties.cloud.provider/action/params` 选择进程内已注册实现。`main` 只自动注册 `aliyun` Provider，当前内置动作聚焦 NAS 文件系统、挂载点和 Kubernetes StorageClass 引导。

仓库内的 `custom` 包是扩展模板，不会自动注册，也不是动态插件系统。新增 Provider 当前需要编译并重启 Eruun。完整当前契约见 [CloudJob 说明](cloudjob-skeleton.md)。

## 2. 目标

AI Provider 集成需要支持但不限于：

- 托管模型推理、embedding、评测或内容处理 API。
- 对象存储、模型制品、数据集和报告存储。
- GPU/计算资源申请及异步生命周期。
- Provider 能力发现、版本、区域和配额约束。
- 凭据隔离、幂等、状态恢复、取消/补偿、成本和审计。

首个目标是验证一个真实能力闭环，不是预先建设通用插件市场、热插拔进程管理器或覆盖所有云的统一最低公分母。

## 3. Provider 能力边界

Provider adapter 应把 Eruun 的稳定意图映射到外部 API，而不是把任意 SDK 调用透传给用户。每个 action 需要声明：

- 稳定名称、版本和所需 Provider 能力。
- 输入校验、Secret/设置引用和敏感字段。
- 幂等身份、外部资源身份和重复调用语义。
- 同步完成、异步轮询、可重试错误和终态错误。
- 取消能否终止外部动作，以及失败后的补偿责任。
- 返回摘要、制品、成本和审计事件。

未知 Provider、action 或不支持的 capability 必须 fail-fast。核心运行时不能在失败时改用另一个 Provider、区域或凭据来源。

## 4. 凭据和授权

- Application/Workflow 只保存凭据引用，不保存 access key、API key 或 refresh token。
- Provider runtime 在执行边界解析凭据，并限制其 workspace、action、区域和生命周期。
- Eruun 登录 Token 不得透传给外部 AI 服务。
- 人类用户委派访问时，使用目标 Provider 支持的授权流程和资源受众；机器调用使用独立 workload identity 或短期凭据。
- Provider 请求、错误、日志、trace、checkpoint 和制品都必须脱敏。
- 修改 Provider 设置、绑定高成本资源或执行高风险 action 需要平台授权和审计。

## 5. 异步执行与恢复

云 API 经常在接受请求后异步完成。Provider action 应能把可序列化 checkpoint 写入 Job 执行记录或受控制品，并在 Worker 重启后重新构造 runtime。

checkpoint 至少需要绑定：

- 当前 Workflow/Job execution identity。
- Provider、action 与实现版本。
- 幂等键和外部资源身份。
- 已完成阶段、下一次查询所需状态和经过脱敏的诊断。

进程内 SDK client、连接、Token 或未序列化对象不能成为恢复所需事实。旧 generation 的异步结果不能推进当前执行。

## 6. 幂等、取消与补偿

- 创建类 action 优先使用 Provider 原生 client token、request ID 或稳定资源名。
- 查询类 action 不应产生隐式创建或配置修改。
- 重试前要区分“请求明确失败”“结果未知”和“外部动作仍在运行”。
- 取消只在 Provider 明确支持时宣称终止；否则停止 Eruun 后续步骤，并记录外部动作仍可能继续。
- 补偿是 action 的显式能力，不能把所有失败都解释成自动删除外部资源。
- 成本较高或不可逆 action 可以要求 approval step。

## 7. 能力发现与版本

目标设计应让部署者知道当前构建和环境支持哪些 Provider/action，以及每个 action 的实现版本和依赖。能力发现至少区分：

- 已编译并注册。
- 凭据/设置已配置。
- 目标区域或账号支持。
- 外部服务可达。
- 当前调用者有权使用。

这些状态不能简化成一个“插件健康”布尔值。运行中的任务应绑定实际 Provider/action 版本；升级后的重试是否允许迁移版本必须由 action 明确决定。

## 8. 与 CloudJob 的关系

CloudJob 已具备 provider/action 分发、多阶段 state 和执行记录，可以作为首个 AI Provider 的实现载体。采用它之前需要验证：

- action 是否适合 Application-scoped Workflow。
- 输入是否能通过结构化、版本化 schema 校验，而不是任意 `params`。
- checkpoint 是否足以跨进程恢复。
- 凭据、成本、审计和结果制品是否有统一边界。
- 取消和清理是否与 Workflow 终态一致。

如果这些需求可以增量完善现有 CloudJob，就不新增第二套 Provider runtime。只有隔离、部署或生命周期证据表明进程内实现不够时，才评估外部进程、RPC 或 operator 形态。

## 9. 首个实现门禁

1. 选择一个明确的 AI action 和一个 Provider，定义最小输入、输出、幂等和权限。
2. 完成成功、参数错误、权限失败、限流、异步等待、结果未知、取消和恢复测试。
3. 验证日志/trace 脱敏、成本归属和审计查询。
4. 用第二个 action 或 Provider 验证抽象是否真正通用，再决定插件部署模型。

升级为 Current 需要可执行示例、Provider 版本矩阵、权限与凭据说明、故障恢复、真实环境验收和运维指标。
