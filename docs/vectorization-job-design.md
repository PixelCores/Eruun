# Eruun 向量化任务方向

> 状态：Draft / Proposal。本文描述 provider-neutral 的数据向量化能力，不代表当前存在 vectorize 路由、组件类型、Go 接口、服务端 flags、数据库表或内置 Milvus/embedding 部署。

> 示例说明：本文流程块仅是概念伪代码，不可直接执行。

## 1. 定位

向量化是 AI Runtime 的批处理数据能力：从受控数据源读取内容，完成解析、切分和 embedding，把向量与可追溯元数据写入目标存储，并产出可查询、可审计的结果摘要。

它应复用 Eruun 的 Workflow、Job、权限、取消、超时、日志和执行 ownership，不创建第二套调度器。当前 `main` 只有通用 Application/Workflow 和 Kubernetes Job 基础，还没有向量化专用实现。

## 2. 目标与非目标

目标：

- 以不可变输入 revision 和处理配置产生可重复的输出。
- 支持可替换的数据源、解析器、embedding endpoint 和向量存储。
- 提供进度、错误、用量、增量/checkpoint 和结果摘要。
- 对源数据、凭据、chunk 内容、embedding 和目标存储执行统一授权和审计。
- 与 Agent 评测、知识检索等上层能力共享制品与任务治理。

非目标：

- 在第一版同时支持所有文档格式、OCR、代码解析和数据库。
- 固定某个 embedding 模型、向量数据库、分块算法或 SDK。
- 把未经实现的 REST 路由、CLI flags、Go 类型或 Kubernetes 清单作为用户说明。
- 在 Eruun 内重新实现一个文档管理或检索产品。

## 3. 目标流水线

```text
resolve authorized source
  -> materialize immutable input manifest
  -> parse and normalize
  -> split into stable chunks
  -> call embedding provider
  -> write vectors and metadata
  -> verify result
  -> publish summary and artifacts
```

每一步都必须有稳定输入摘要和可观察输出。解析、embedding 或写入失败时，任务应保留足够 checkpoint 以判断安全重试范围，但不能把源正文或凭据写入普通 JobInfo。

## 4. 最小输入与输出

最小输入概念包括：

- workspace/project 归属。
- 数据源引用及不可变 revision、ETag 或内容摘要。
- 解析与分块策略的版本化引用。
- embedding endpoint、模型 revision 和凭据引用。
- 向量存储目标、collection/index 身份和凭据引用。
- 资源、预算、截止时间和制品保留策略。

最小输出概念包括：

- 文档、chunk、成功、跳过和失败数量。
- 输入、处理配置、embedding 模型和输出 revision。
- Token/请求用量、延迟和重试摘要。
- 失败样本清单与受控诊断制品。
- 可选 checkpoint 与目标写入校验结果。

这些是能力要求，不是已经决定的 JSON 或数据库字段。

## 5. Provider 边界

### 数据源

首个实现只选择一种可版本化且可授权的数据源。后续可以增加对象存储、HTTPS、Git、ConfigMap 或平台数据集，但每种来源都必须提供稳定 revision、大小限制、超时、内容类型校验和 SSRF/路径安全边界。

### 解析与分块

解析器负责把输入转换为带来源位置的规范文本；分块器从规范文本产生稳定 chunk ID。实现必须记录解析器/分块器版本，以免相同输入在升级后静默得到不同输出。

### Embedding

Embedding Provider 只需要表达批量输入、模型 revision、维度、用量和错误分类。它可以是集群内服务或外部 API，不应在核心领域类型中绑定具体厂商。

### 向量存储

向量存储 Provider 负责 collection/index 检查、批量 upsert、幂等身份、删除或替换策略和结果验证。目标存储的 schema 与距离度量属于任务配置或 Provider 能力，不是 Eruun 的全局常量。

## 6. 幂等、增量与恢复

- 输入 revision、处理配置摘要、embedding 模型 revision 和 chunk ID 共同决定输出身份。
- 重试必须避免同一 chunk 产生不可追溯的重复记录。
- 增量任务要明确新增、变更和删除文档的处理方式；不能只追加而永久保留已删除内容。
- checkpoint 只能复用与当前输入和配置摘要匹配的进度。
- 外部写入无法提供事务时，Provider 必须定义幂等 upsert、staging collection 或补偿策略。

第一版可以只支持全量、单分片执行；只有真实数据量证明需要后，再增加并行分片和增量同步。

## 7. 权限与数据治理

- 源数据、embedding 服务和向量存储使用彼此独立的凭据引用。
- Runner 默认不获得 Kubernetes API Token；必要访问使用任务作用域 ServiceAccount。
- 出站网络只开放声明的数据源、embedding、目标存储和制品端点。
- 原文、chunk 和 embedding 默认不写入日志或指标 label。
- 报告下载、删除和保留必须重新校验 workspace/project 权限。
- 数据源许可、个人信息和保留策略属于调用方治理输入，Eruun 不因技术可访问而自动获得使用授权。

## 8. 与现有 Eruun 的映射

可直接复用：

- Workflow 的步骤、取消、超时、审批、回调、lease 和 fencing。
- Kubernetes `job`、Secret/envFrom、storage、resources 和 securityPolicy。
- 账号、workspace 授权和统一错误响应。

需要实现并验证：

- 版本化任务输入和结果摘要。
- Runner 或步骤间制品协议。
- Provider 接口与能力发现。
- 进度、checkpoint、目标写入幂等和数据删除。
- 对大型制品的 ArtifactStore 与访问控制。

## 9. 实施门禁

1. 选定一种纯文本输入、一个 OpenAI-compatible embedding endpoint 和一个目标存储，完成小数据集闭环。
2. 验证稳定 chunk identity、重试幂等、权限、取消和报告。
3. 再增加一种解析格式和增量更新，确认 Provider 边界没有泄漏厂商细节。
4. 通过真实数据量决定并行、分片、容量调度和 checkpoint 复杂度。

升级为 Current 前必须提供可执行示例、实现与测试、数据治理说明、故障恢复证据和运维指标。
