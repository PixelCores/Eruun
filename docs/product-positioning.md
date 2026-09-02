# Eruun 产品定位

> 状态：Current。本文定义 Eruun 的产品定位、当前价值与能力边界；具体 API、工作流和部署行为以对应的 Current 文档及代码为准。

## 定位声明

**A distributed runtime for agents, models, and AI workloads.**

**面向 Agent、模型与 AI 工作负载的分布式运行时。**

Eruun 为需要在 Kubernetes 上部署和运行 AI 应用的团队提供统一的服务端运行时。它以声明式组件、traits 和持久化工作流描述应用，将 API 接入、任务调度、运行状态与 Kubernetes 资源调和连接成一条可追踪的执行链路。

“Agents、models 和 AI workloads”描述 Eruun 服务的工作负载范围，不表示当前版本已经为每一类场景提供独立的高层产品或专用 API。当前实现的共同基础是 Kubernetes 应用运行与工作流执行能力。

## Eruun 解决的问题

AI 应用通常不只包含一个模型进程，还会组合 API 服务、有状态组件、后台任务、存储、网络、权限和多阶段操作。直接拼接 Kubernetes 资源与脚本，会使部署意图、执行状态和故障恢复分散在不同系统中。

Eruun 提供统一的运行时边界：

- 使用组件、traits 和 workflow 表达应用结构与运行意图。
- 通过 REST API 提供应用生命周期、校验、状态和运维入口。
- 以持久化工作流组织多步骤任务，并保留可查询的执行状态。
- 将 API、controller、scheduler 和 worker 职责拆分，使运行角色可以独立部署和扩缩。
- 调和 Kubernetes workload、Service、存储、RBAC、Ingress、探针和其他运行资源。
- 使用数据库中的工作流状态与执行 ownership 支持分布式执行和故障恢复。

## 面向的工作负载

| 工作负载 | Eruun 提供的共同运行时基础 | 当前边界 |
| --- | --- | --- |
| Agent | 服务、依赖组件、配置、权限和生命周期工作流的部署与运行 | 不提供 Agent 编排框架、推理协议或 Agent SDK |
| Model | 模型服务及其存储、网络、资源和运行状态的 Kubernetes 承载 | 分布式模型服务设计只有标记为 Current 时才属于已实现契约 |
| AI workload | 数据处理、评估、向量化等任务所需的声明式组件和工作流基础 | 具体任务类型只有标记为 Current 时才属于已实现能力 |

这三类工作负载共享同一运行时模型，不需要在 Eruun 内复制三套生命周期、调度或资源管理实体。

## 当前产品形态

Eruun 当前是 Go 实现的 API Server 与 Kubernetes 工作流运行时，提供：

- OAM 风格的组件、traits 和 workflow 应用模型。
- 应用生命周期、工作流执行、校验、状态、日志、文件、Shell、系统设置和 namespace 纳管 API。
- 基于 Redis Streams 或 Kafka 的工作流消息传输。
- 分离的 API、controller、scheduler 和 worker 运行职责。
- Kubernetes 资源生成、调和、等待、状态同步与清理能力。
- Helm 与独立 Manifest 部署路径。

Eruun 只交付服务端运行时，不包含客户端命令行应用。

## 产品边界

以下内容不应从定位声明推导为当前已实现能力：

- Draft / Proposal 文档中的 Agent evaluation、向量化、分布式模型服务或全局调度设计。
- 跨 Kubernetes 集群调度、跨地域多活或任意基础设施的通用编排。
- 对外部系统副作用的 exactly-once 保证。
- Agent 框架、模型训练框架、推理引擎或完整 AI 开发平台所承担的能力。

Eruun 可以承载这些系统产生的工作负载，但不会替代它们的领域逻辑。新增能力只有在代码实现、验证材料和文档状态一致后，才应加入 Current 产品契约。

## 对外表述

推荐使用以下一致表述：

> Eruun is a distributed runtime for agents, models, and AI workloads. It provides a declarative Kubernetes application model, durable workflows, distributed runtime roles, and operational APIs for deploying and operating AI applications.

中文表述：

> Eruun 是面向 Agent、模型与 AI 工作负载的分布式运行时，通过声明式 Kubernetes 应用模型、持久化工作流、分布式运行角色和运维 API，帮助团队部署与运行 AI 应用。

对外介绍当前能力时，应链接 [文档索引](README.md) 并遵循其中的状态约定，不把 Draft / Proposal 表述为已发布功能。
