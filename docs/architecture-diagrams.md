# Eruun 当前架构图

> 状态：Implemented Reference。图中只展示 `main` 已实现的运行关系；未来 AI Runtime 能力见 [AI Runtime 愿景](ai-runtime-vision.md)。

## 1. 四角色与依赖

```mermaid
flowchart TB
    Client[API 调用方]

    subgraph Runtime[Eruun runtime]
        API[api\nHTTP、认证授权、任务创建]
        Controller[controller\nKubernetes 观察、状态投影]
        Scheduler[scheduler\n派发、lease reaper]
        Worker[worker\nWorkflow 与 Job 执行]
    end

    DB[(MySQL\n领域状态、Workflow ownership)]
    Redis[(Redis\n缓存、应用锁、取消信号、可选消息)]
    Kafka[(Kafka\n可选消息后端)]
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
```

关键边界：

- Scheduler 而不是 Worker 扫描并派发 waiting Workflow。
- Worker 消费 dispatch 后仍需通过数据库 ownership 条件认领执行。
- MySQL 是 Workflow 状态和 lease 的事实源；Redis/Kafka 只承载协调或消息。
- Controller 负责全局 Kubernetes 状态投影，Worker 使用本地 observer 等待自己创建的 workload。

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

    C->>A: 创建/执行应用或 Workflow
    A->>D: 保存领域对象与 waiting task
    A-->>C: 返回 appId/taskId
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

## 3. Application、Component、Trait 与 Workflow

```mermaid
flowchart LR
    App[Application\nworkspace 归属]
    Components[Components\nwebservice/store/job/scheduledjob/config/secret/cloudjob]
    Traits[Traits\nstorage/env/resources/security/RBAC/...]
    Workflow[Workflow\nStepByStep 或 DAG]
    JobTask[JobTask\n内部执行动作]
    Resources[Kubernetes 或 Cloud action]

    App --> Components
    Components --> Traits
    App --> Workflow
    Workflow --> JobTask
    Components --> JobTask
    Traits --> JobTask
    JobTask --> Resources
```

- Component 描述要运行或生成的对象。
- Trait 为 Component 增加正交能力；它不是独立运行实体。
- Workflow 引用 Component 并定义执行顺序、审批和失败策略。
- JobTask 是内部执行记录，不是新的用户侧顶层队列。

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
    Current[Current / Implemented Reference\n代码与测试可验证]
    Proposal[Draft / Proposal\n方向、缺口、决策门禁]
    Implementation[实现 PR\nAPI、代码、测试、运维证据]
    Promoted[升级为 Current]

    Current --> Proposal
    Proposal --> Implementation
    Implementation --> Promoted
```

Agent、MCP/CLI、评测、向量化、vLLM/HAMi 和通用 AI Provider 当前都位于 Proposal 一侧。示意图不能让它们越过实现与验证门禁。
