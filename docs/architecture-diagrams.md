# Eruun 当前架构图

> 状态：Implemented Reference。图中只展示 `main` 已实现的运行关系；未来 AI Runtime 能力见 [AI Runtime 愿景](ai-runtime-vision.md)。

## 1. 四角色与依赖

```mermaid
flowchart TB
    Client[API 调用方]

    subgraph Runtime[Eruun runtime]
        API["api<br/>HTTP/gRPC、认证授权、任务创建"]
        Controller["controller<br/>观察、状态投影、延迟任务与结果协调"]
        Scheduler["scheduler<br/>派发、lease reaper"]
        Worker["worker<br/>Workflow 与 Job 执行"]
    end

    DB[("MySQL<br/>领域状态、Workflow ownership")]
    Redis[("Redis<br/>缓存、应用锁、取消信号、可选消息")]
    Kafka[("Kafka<br/>可选消息后端")]
    K8s[Kubernetes API]

    Client --> API
    API --> DB
    API --> Redis
    Scheduler <--> DB
    Scheduler --> Redis
    Scheduler --> Kafka
    Redis --> Worker
    Kafka --> Worker
    Worker <--> DB
    Worker --> Redis
    Worker --> K8s
    Controller <--> K8s
    Controller --> DB
    Controller <--> Redis
    Controller <--> Kafka
```

关键边界：

- Scheduler 扫描并派发 waiting task，应用 Workflow 与独立空间 Job 共用该链路。
- Worker 消费 dispatch 后仍需通过数据库 ownership 条件认领执行。
- MySQL 是 Workflow 状态和 lease 的事实源；Redis/Kafka 只承载协调或消息。
- Controller 负责全局 Kubernetes 状态投影、延迟任务和结果协调；Worker 使用本地 observer 等待自己创建的 workload。Controller 与 Scheduler 分别使用独立 Kubernetes Lease 选主。
- Redis Streams 与 Kafka 是可选消息后端；选择 Kafka 后，Redis 仍承担缓存、应用锁和取消信号。上图展示主要依赖，非完整网络访问矩阵。

## 2. Workflow 执行与恢复

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API
    participant D as MySQL
    participant S as Scheduler leader
    participant Q as Redis/Kafka
    participant W as Worker
    participant K as Kubernetes
    participant O as Controller

    C->>A: 提交应用执行、独立 command 或带 traits.eval 的 job
    A->>D: 保存 waiting task 及所属应用/空间
    A-->>C: 返回 taskId
    S->>D: CAS waiting，生成 generation/token
    S->>Q: 发布版本化 dispatch
    Q->>W: 交付 dispatch
    W->>D: 按 generation/token 认领并续租
    W->>K: 创建或调和资源
    W->>D: 保存 Job 与 Workflow 进度
    O->>K: List/Watch 运行资源
    O->>D: 投影组件运行状态
    W->>D: 写入终态并释放 ownership
```

故障恢复遵循以下原则：

1. 队列允许重复交付，Worker 只接受当前 generation/token。
2. Worker 运行期间按数据库时间续租。
3. Scheduler 回收过期且身份完整的执行租约，并把任务恢复为 waiting。
4. 旧 generation 的迟到结果不能覆盖新执行。

独立空间 Job 不创建 Application 或 Component，由 Worker 将任务声明构建为 Kubernetes Job。评测使用 `type: job` + `traits.eval`，也可作为应用 Workflow 的顶层 `job` 组件执行。Harbor Runner 在 Pod 内先认领执行，再下载任务包、运行评测、上报阶段/心跳/终态并上传完整原始结果。API 保存源结果后，Controller 独立执行 MinIO/数据库目标保存与过期清理；保存失败不改变已完成的评测事实，重试保存也不会重跑评测。同一应用任务内的评测结果以 Job `executionKey` 区分。见 [空间 Job API](workspace-jobs-api.md)。

上图展示立即执行主路径；延迟 Job 由 Worker 写入数据库检查点，Controller 到期后创建 Kubernetes Job，结合 delay/result 消息与 outbox 恢复执行进度。

## 3. Application、Component、Trait 与 Workflow

```mermaid
flowchart LR
    App["Application<br/>workspace 归属"]
    Components["Components<br/>webservice / store / job / scheduledjob<br/>config / secret / cloudjob"]
    Traits["Traits<br/>storage / env / resources / security / RBAC / …"]
    Workflow["Workflow<br/>StepByStep 或 DAG"]
    WorkspaceJob["独立空间 Job<br/>command 或 job + traits.eval，无 AppID"]
    JobTask["JobTask<br/>内部执行动作"]
    Resources[Kubernetes 或 Cloud action]

    App --> Components
    Components --> Traits
    App --> Workflow
    Workflow --> JobTask
    Components --> JobTask
    Traits --> JobTask
    WorkspaceJob --> JobTask
    JobTask --> Resources
```

- Component 描述要运行或生成的对象。
- Trait 为 Component 增加正交能力；它不是独立运行实体。
- 图中包含引擎内部扩展；空间业务路径拒绝 CloudJob、额外 RBAC、任意 ServiceAccount、host/跨 namespace 操作及 NodePort/LoadBalancer，不能仅凭类型或 Trait 的存在判断可用权限。
- Workflow 引用 Component 并定义执行顺序、审批和失败策略。
- 独立空间 Job 直接声明任务规格，复用相同的调度、执行租约和 Job 生命周期。
- JobTask 是内部执行计划，JobInfo 保存执行明细；它们不形成新的用户侧顶层队列。

## 4. Trait 到 Kubernetes 的映射

```mermaid
flowchart TB
    Spec[domain/spec.Traits]
    Validate[Domain validation]
    Process[workflow/traits processors]
    Result[TraitResult aggregation]
    Workload[PodTemplate mutation]
    Extra[Additional Kubernetes objects]
    Jobs[resource Job controllers]

    Spec --> Validate
    Validate --> Process
    Process --> Result
    Result --> Workload
    Result --> Extra
    Workload --> Jobs
    Extra --> Jobs
```

| Trait 结果 | 当前目标 |
| --- | --- |
| volumes / mounts | PodTemplate、PVC 或 volumeClaimTemplate |
| env / envFrom | 主容器、init container 或 sidecar |
| resources / probes / securityContext | 容器字段 |
| nodeSelector / ServiceAccount / rollout | Pod 或 workload 字段 |
| ingress / RBAC 等 AdditionalObjects | 独立 Kubernetes 资源 Job |
| service / share | Service 生成及资源调和/清理路径 |

## 5. 当前与未来的文档分界

```mermaid
flowchart LR
    Current["Current / Implemented Reference<br/>代码与测试可验证"]
    Proposal["Draft / Proposal<br/>方向、缺口、决策门禁"]
    Implementation["实现 PR<br/>API、代码、测试、运维证据"]
    Promoted[升级为 Current]

    Current --> Proposal
    Proposal --> Implementation
    Implementation --> Promoted
```

独立 `command` Job、独立与应用 Workflow 内的 Harbor `traits.eval` 评测、Runner 阶段协议与结果保存属于已实现能力。通用 Agent 生命周期、MCP/CLI 工具管理、更多评测框架、向量化、vLLM/HAMi 和通用 AI Provider 仍位于 Proposal 一侧。示意图不能让它们越过实现与验证门禁。
