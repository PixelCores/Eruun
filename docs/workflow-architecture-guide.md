# Eruun 工作流引擎架构详解


> 企业角色拆分、双 Leader Election、数据库 generation/token lease 与 60 秒恢复边界见 [企业级分布式运行时设计](enterprise-distributed-runtime-design.md)；本文仍聚焦 Workflow/Job 内部执行结构。

## 目录

- [1. 概述与背景](#1-概述与背景)
- [2. 设计理念](#2-设计理念)
- [3. 设计原则](#3-设计原则)
- [4. 架构设计](#4-架构设计)
- [5. 核心组件详解](#5-核心组件详解)
- [6. 执行流程](#6-执行流程)
- [7. 分布式支持](#7-分布式支持)
- [8. 取消与清理机制](#8-取消与清理机制)
- [9. 状态管理](#9-状态管理)
- [10. 并发控制](#10-并发控制)
- [11. 配置参考](#11-配置参考)
- [12. 优势总结](#12-优势总结)

---

## 1. 概述与背景

### 1.1 工作流引擎的定位

Eruun 工作流引擎是整个应用交付系统的核心组件，负责将用户声明的应用配置转换为实际运行在 Kubernetes 集群上的资源。它充当了"编排者"的角色，协调多个组件的创建、更新和删除操作，确保应用的正确部署。

工作流引擎解决了以下核心问题：

- **资源依赖管理**：确保 ConfigMap、Secret、PVC 等依赖资源先于 Deployment、StatefulSet 创建
- **执行顺序控制**：支持串行和并行两种执行模式，满足不同场景需求
- **状态追踪**：完整记录每个任务和 Job 的执行状态，便于问题排查
- **故障恢复**：支持任务重试、取消和资源清理，保证系统一致性
- **分布式扩展**：支持多实例部署，通过 Redis Streams 实现任务分发

### 1.2 与 OAM 的集成

工作流引擎借鉴了 OAM (Open Application Model) 的设计思想，支持声明式的工作流定义：

```json
{
  "workflow": [
    {
      "name": "config-step",
      "mode": "StepByStep",
      "components": ["config", "secret"]
    },
    {
      "name": "database",
      "mode": "DAG",
      "components": ["mysql", "redis"]
    },
    {
      "name": "services",
      "mode": "StepByStep",
      "components": ["backend", "frontend"]
    }
  ]
}
```

工作流支持两种执行模式：

| 模式 | 标识 | 说明 |
|------|------|------|
| 串行模式 | `StepByStep` | 组件按声明顺序依次执行，前一个完成后才执行下一个 |
| 并行模式 | `DAG` | 同一 Step 内的组件并行执行，适合无依赖关系的组件 |

---

## 2. 设计理念

### 2.1 声明式编排

工作流采用声明式的方式定义组件编排顺序，用户只需声明"做什么"，系统自动处理"怎么做"：

```go
// model/workflow.go
type WorkflowStep struct {
    Name         string              `json:"name"`
    Mode         config.WorkflowMode `json:"mode,omitempty"`
    Properties   []Policies          `json:"properties,omitempty"`
    SubSteps     []*WorkflowSubStep  `json:"subSteps,omitempty"`
}
```

这种设计的优势：
- **简化用户操作**：用户无需关心底层资源创建的复杂逻辑
- **提高可维护性**：工作流定义与执行逻辑分离
- **增强可读性**：工作流结构清晰，易于理解和调试

### 2.2 组件化 Job

每种 Kubernetes 资源类型都有对应的 Job 控制器，实现了高度的模块化：

```
pkg/apiserver/event/workflow/job/
├── job.go                 # Job 执行器核心
├── job_deploy.go          # Deployment 控制器
├── job_statefulset.go     # StatefulSet 控制器
├── job_service.go         # Service 控制器
├── job_pvc.go             # PVC 控制器
├── job_configmap.go       # ConfigMap 控制器
├── job_secret.go          # Secret 控制器
├── job_ingress.go         # Ingress 控制器
├── job_rbac.go            # RBAC 资源控制器
├── cleanup_tracker.go     # 清理跟踪器
└── naming.go              # 命名规范
```

每个 Job 控制器实现统一的接口：

```go
// job/job.go
type JobCtl interface {
    Run(ctx context.Context) error      // 执行资源创建/更新
    Clean(ctx context.Context)          // 清理已创建的资源
    SaveInfo(ctx context.Context) error // 保存执行信息到数据库
}
```

### 2.3 可观测性优先

工作流引擎深度集成了 OpenTelemetry 分布式追踪：

```go
// controller.go
func (w *WorkflowCtl) Run(ctx context.Context, concurrency int) error {
    tracer := otel.Tracer("workflow-runner")
    ctx, span := tracer.Start(ctx, workflowName, trace.WithAttributes(
        attribute.String("workflow.name", workflowName),
        attribute.String("workflow.task_id", taskMeta.TaskID),
    ))
    defer span.End()
    
    // 创建带 traceID 的 logger
    logger := klog.FromContext(ctx).WithValues(
        "traceID", span.SpanContext().TraceID().String(),
        "workflowName", workflowName,
        "taskID", taskMeta.TaskID,
    )
    // ...
}
```

每个 Job 执行也会创建子 Span：

```go
// job/job.go
func runJob(ctx context.Context, job *model.JobTask, ...) {
    tracer := otel.Tracer("job-runner")
    ctx, span := tracer.Start(ctx, job.Name, trace.WithAttributes(
        attribute.String("job.name", job.Name),
        attribute.String("job.type", job.JobType),
    ))
    defer span.End()
    // ...
}
```

### 2.4 弹性与容错

工作流引擎当前要求显式配置外部消息队列，可根据部署规模在两种分布式后端之间选择：

| 模式 | 适用场景 | 特点 |
|------|----------|------|
| Redis 分布式 | 中小规模部署、低延迟场景 | 使用 Redis Streams，支持任务分发和故障恢复 |
| Kafka 分布式 | 大规模部署、高吞吐场景 | 使用 Kafka，支持高吞吐任务分发和重平衡恢复 |

启动逻辑：

```go
// event/workflow/workflow.go 与 dispatcher.go
func (w *Workflow) StartController(ctx context.Context, errChan chan error)
func (w *Workflow) StartScheduler(ctx context.Context, errChan chan error)
func (w *Workflow) StartWorker(
    consumerCtx context.Context,
    executionCtx context.Context,
    errChan chan error,
)
```

---

## 3. 设计原则

### 3.1 单一职责原则

每个 Job 控制器只负责一种资源类型的生命周期管理：

```go
// job/job.go
func initJobCtl(job *model.JobTask, ...) JobCtl {
    switch job.JobType {
    case string(config.JobDeploy):
        return NewDeployJobCtl(job, client, store, ack)
    case string(config.JobDeployService):
        return NewDeployServiceJobCtl(job, client, store, ack)
    case string(config.JobDeployStore):
        return NewDeployStatefulSetJobCtl(job, client, store, ack)
    case string(config.JobDeployPVC):
        return NewDeployPVCJobCtl(job, client, store, ack)
    case string(config.JobDeployConfigMap):
        return NewDeployConfigMapJobCtl(job, client, store, ack)
    case string(config.JobDeploySecret):
        return NewDeploySecretJobCtl(job, client, store, ack)
    // ... 更多类型
    }
}
```

### 3.2 优先级调度

资源按依赖关系分为不同优先级，确保依赖资源先创建：

```go
// config/consts.go
const (
    JobPriorityMaxHigh = 0   // 最高优先级：Secret, ConfigMap
    JobPriorityHigh    = 1   // 高优先级：PVC, ServiceAccount, Role
    JobPriorityNormal  = 10  // 普通优先级：Deployment, StatefulSet, Service
    JobPriorityLow     = 20  // 低优先级：清理任务、通知任务
)
```

Job 构建时自动分配优先级：

```go
// job_builder.go
func buildJobsForComponent(ctx context.Context, component *model.ApplicationComponent, ...) map[int][]*model.JobTask {
    buckets := newJobBuckets()
    
    switch component.ComponentType {
    case config.ConfJob:
        // ConfigMap 分配到最高优先级
        buckets[config.JobPriorityMaxHigh] = append(buckets[config.JobPriorityMaxHigh], jobTask)
    case config.SecretJob:
        // Secret 分配到最高优先级
        buckets[config.JobPriorityMaxHigh] = append(buckets[config.JobPriorityMaxHigh], jobTask)
    case config.ServerJob:
        // Deployment 相关的附加资源（PVC、Ingress）分配到高优先级
        // Deployment 本身分配到普通优先级
        buckets[config.JobPriorityNormal] = append(buckets[config.JobPriorityNormal], jobTask)
    }
    
    return buckets
}
```

执行时按优先级顺序处理：

```go
// controller.go
for _, stepExec := range stepExecutions {
    priorities := sortedPriorities(stepExec.Jobs) // [0, 1, 10, 20]
    for _, priority := range priorities {
        tasksInPriority := stepExec.Jobs[priority]
        // 执行该优先级的所有 Job
        job.RunJobs(ctx, tasksInPriority, stepConcurrency, ...)
    }
}
```

### 3.3 幂等性设计

Job 控制器在执行前会检查资源是否存在，支持重复执行：

```go
// job_deploy.go
func (c *DeployJobCtl) run(ctx context.Context) error {
    deployLast, isAlreadyExists, err := c.deploymentExists(ctx, deployName, deploy.Namespace)
    if err != nil {
        return fmt.Errorf("failed to check deployment existence: %w", err)
    }

    if isAlreadyExists {
        // 已存在：检查是否需要更新
        if isDeploymentChanged(deployLast, deploy) {
            // 使用 Server-Side Apply 更新
            updated, err := c.ApplyDeployment(ctx, deploy)
            // ...
        } else {
            klog.Infof("Deployment %q is up-to-date, skip apply.", deploy.Name)
        }
        // 标记为"已观察"而非"已创建"，避免清理时误删
        markResourceObserved(ctx, config.ResourceDeployment, deploy.Namespace, deploy.Name)
    } else {
        // 不存在：创建新资源
        result, err := c.client.AppsV1().Deployments(deploy.Namespace).Create(ctx, deploy, ...)
        // 标记为"已创建"，失败时需要清理
        MarkResourceCreated(ctx, config.ResourceDeployment, deploy.Namespace, deploy.Name)
    }
    return nil
}
```

Deployment 的 `rollout` trait 参与幂等判断：

- `traits.rollout` 在 API 校验阶段先被约束为明确配置：`webservice` 的 `RollingUpdate` 必须包含 `rollingUpdate.maxSurge` 与 `rollingUpdate.maxUnavailable`，`Recreate` 不允许包含 `rollingUpdate`。
- Job 构建阶段由 `GenerateWebService` 将 `traits.rollout` 渲染到 `Deployment.spec.strategy`。
- 未配置 `traits.rollout` 时，期望 Deployment 不声明 `spec.strategy`；若线上 Deployment 仍保留 `Recreate` 或自定义 rolling 参数，`isDeploymentChanged` 会把它识别为需要 reset 的差异。
- 已经是 Kubernetes 默认滚动策略的 Deployment 不会因为 API Server 默认化出的 `maxSurge/maxUnavailable=25%` 被重复 patch。

### 3.4 状态驱动

任务生命周期由状态机管理：

```
┌─────────┐    创建任务    ┌─────────┐    获取到执行权    ┌─────────┐
│ waiting │ ─────────────> │ queued  │ ────────────────> │ running │
└─────────┘                └─────────┘                   └─────────┘
                                                              │
                    ┌─────────────────────────────────────────┼─────────────────────┐
                    │                                         │                     │
                    ▼                                         ▼                     ▼
              ┌───────────┐                            ┌───────────┐         ┌───────────┐
              │ completed │                            │  failed   │         │ cancelled │
              └───────────┘                            └───────────┘         └───────────┘
                                                              │
                                                              ▼
                                                       ┌───────────┐
                                                       │  timeout  │
                                                       └───────────┘
```

状态定义：

```go
// config/consts.go
const (
    StatusWaiting   Status = "waiting"   // 等待执行
    StatusQueued    Status = "queued"    // 已入队，等待 worker 处理
    StatusRunning   Status = "running"   // 执行中
    StatusCompleted Status = "completed" // 执行完成
    StatusFailed    Status = "failed"    // 执行失败
    StatusTimeout   Status = "timeout"   // 执行超时
    StatusCancelled Status = "cancelled" // 已取消
)
```

---

## 4. 架构设计

### 4.1 整体架构图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              API Layer                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  POST /applications/:appID/workflow/exec                                │ │
│  │  POST /applications/:appID/workflow/cancel                              │ │
│  │  GET  /workflow/tasks/:taskID/status                                    │ │
│  │  GET  /workflow/tasks/:taskID/stages                                    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Service Layer                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  WorkflowService                                                        │ │
│  │  - CreateWorkflowTask()    创建工作流任务                                 │ │
│  │  - ExecWorkflowTask()      触发工作流执行                                 │ │
│  │  - CancelWorkflowTask()    取消工作流任务                                 │ │
│  │  - GetTaskStatus()         查询任务状态                                   │ │
│  │  - GetTaskStages()         查询任务阶段详情                               │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Workflow Engine                                      │
│  ┌────────────────────────────┐    ┌────────────────────────────────────┐   │
│  │       Workflow             │    │       WorkflowController            │   │
│  │  ┌──────────────────────┐  │    │  ┌──────────────────────────────┐  │   │
│  │  │ StartController()    │  │    │  │ Run()                        │  │   │
│  │  │ StartScheduler()     │  │    │  │ - GenerateJobTasks()         │  │   │
│  │  │ - Dispatch/Reaper    │──┼───>│  │ - RunJobs() by priority      │  │   │
│  │  │ StartWorker()        │  │    │  │ - updateWorkflowStatus()     │  │   │
│  │  └──────────────────────┘  │    │  └──────────────────────────────┘  │   │
│  └────────────────────────────┘    └────────────────────────────────────┘   │
│                                                   │                          │
│                                                   ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         Job Controllers                               │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │   │
│  │  │ Deploy   │ │ Store    │ │ Service  │ │ PVC      │ │ Config   │   │   │
│  │  │ JobCtl   │ │ JobCtl   │ │ JobCtl   │ │ JobCtl   │ │ JobCtl   │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘   │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │   │
│  │  │ Secret   │ │ Ingress  │ │ SA       │ │ Role     │ │ Binding  │   │   │
│  │  │ JobCtl   │ │ JobCtl   │ │ JobCtl   │ │ JobCtl   │ │ JobCtl   │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
┌──────────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│     MySQL            │  │   Redis          │  │   Kubernetes         │
│  ┌────────────────┐  │  │  ┌────────────┐  │  │  ┌────────────────┐  │
│  │ workflow       │  │  │  │ Streams    │  │  │  │ Deployments    │  │
│  │ workflow_queue │  │  │  │ (任务分发)  │  │  │  │ StatefulSets   │  │
│  │ job_info       │  │  │  ├────────────┤  │  │  │ Services       │  │
│  │ components     │  │  │  │ Cancel     │  │  │  │ ConfigMaps     │  │
│  └────────────────┘  │  │  │ Signals    │  │  │  │ Secrets        │  │
└──────────────────────┘  │  └────────────┘  │  │  │ PVCs           │  │
                          └──────────────────┘  │  └────────────────┘  │
                                                └──────────────────────┘
```

### 4.2 数据模型

本节代码片段展示内部 Domain/DB 模型，不代表对外 API JSON 契约；对外响应以 DTO 的 camelCase 字段为准。

#### Workflow - 工作流定义

```go
// domain/model/workflow.go
type Workflow struct {
    ID           string                  `json:"id" gorm:"primaryKey"`
    Name         string                  `json:"name"`
    Namespace    string                  `json:"namespace"`
    Alias        string                  `json:"alias"`
    Disabled     bool                    `json:"disabled"`
    ProjectID    string                  `json:"project_id"`
    AppID        string                  `json:"app_id"`
    UserID       string                  `json:"user_id"`
    Description  string                  `json:"description"`
    WorkflowType config.WorkflowTaskType `json:"workflow_type"`
    Status       config.Status           `json:"status"`
    Steps        *JSONStruct             `json:"steps,omitempty" gorm:"serializer:json"`
    BaseModel
}
```

#### WorkflowQueue - 任务队列

```go
// domain/model/workflow_queue.go
type WorkflowQueue struct {
    TaskID              string                  `gorm:"primaryKey" json:"task_id"`
    ProjectID           string                  `json:"projectId"`
    WorkflowName        string                  `json:"workflow_name"`
    AppID               string                  `json:"app_id"`
    WorkflowID          string                  `json:"workflow_id"`
    WorkflowDisplayName string                  `json:"workflow_display_name"`
    Status              config.Status           `json:"status,omitempty"`
    TaskCreator         string                  `json:"task_creator,omitempty"`
    TaskRevoker         string                  `json:"task_revoker,omitempty"`
    Type                config.WorkflowTaskType `json:"type,omitempty"`
    BaseModel
}
```

#### JobTask - Job 任务定义

```go
// domain/model/job.go
type JobTask struct {
    Name       string        `json:"name"`
    Namespace  string        `json:"namespace"`
    WorkflowID string        `json:"workflow_id"`
    ProjectID  string        `json:"project_id"`
    AppID      string        `json:"app_id"`
    TaskID     string        `json:"task_id"`
    JobType    string        `json:"job_type"`
    Status     config.Status `json:"status"`
    Timeout    int64         `json:"timeout"`
    StartTime  int64         `json:"start_time"`
    EndTime    int64         `json:"end_time"`
    Error      string        `json:"error"`
    JobInfo    interface{}   `json:"job_info"` // 存储具体的 K8s 资源对象
}
```

### 4.3 消息队列抽象

工作流引擎抽象了消息队列接口，支持多种实现：

```go
// infrastructure/messaging/queue.go
type Queue interface {
    // 确保消费者组存在
    EnsureGroup(ctx context.Context, group string) error
    
    // 入队：将消息推送到流
    Enqueue(ctx context.Context, payload []byte) (string, error)
    
    // 读取：从消费者组读取消息
    ReadGroup(ctx context.Context, group, consumer string, count int, block time.Duration) ([]Message, error)
    
    // 确认：标记消息已处理
    Ack(ctx context.Context, group string, ids ...string) error
    
    // 自动认领：认领空闲超时的待处理消息
    AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, count int) ([]Message, error)
    
    // 关闭连接
    Close(ctx context.Context) error
    
    // 统计信息
    Stats(ctx context.Context, group string) (backlog int64, pending int64, err error)
}
```

目前支持两种实现：

| 实现 | 说明 | 使用场景 |
|------|------|----------|
| RedisStreams | 基于 Redis Streams 的分布式队列 | 生产环境，多实例部署 |
| KafkaQueue | 基于 Apache Kafka 的分布式队列 | 大规模生产环境，需要高吞吐量 |

---

## 5. 核心组件详解

### 5.1 Workflow - 角色化运行入口

`Workflow` 是四类进程共享的实现，但每个进程只启动自己角色的方法：

```go
// event/workflow/workflow.go 与 dispatcher.go
func (w *Workflow) StartController(ctx context.Context, errChan chan error) {
    // delay dispatcher、result dispatcher、result outbox
}

func (w *Workflow) StartScheduler(ctx context.Context, errChan chan error) {
    // schedule dispatcher、waiting task dispatcher、database lease reaper
}

func (w *Workflow) StartWorker(
    consumerCtx context.Context,
    executionCtx context.Context,
    errChan chan error,
) {
    // 消费版本 2 dispatch，认领 DB ownership 后执行任务
}
```

Controller 与 Scheduler 的入口只在各自 Leader 任期中运行；Worker 不参加 Leader Election。`all` 角色会在同一进程内启动三类入口，但仍使用 Controller/Scheduler 双 Lease 和同一套 v2 ownership。

任务恢复不执行启动时全表重置：Scheduler reaper 只以 CAS 回收已过期且 ownership 完整的 `queued/running` execution lease，下一次派发生成新的 `runGeneration/runToken`。Worker 关闭时先停止 intake，再用独立 `executionCtx` 排空已启动任务。

### 5.2 WorkflowController - 任务控制器

`WorkflowCtl` 负责单个工作流任务的执行控制：

```go
// event/workflow/controller.go
type WorkflowCtl struct {
    workflowTask             *model.WorkflowQueue
    workflowTaskMutex        sync.RWMutex
    Client                   kubernetes.Interface
    Store                    datastore.DataStore
    prefix                   string
    ack                      func()                  // 状态同步回调
    defaultJobTimeoutSeconds int64
    ctx                      context.Context
}
```

#### Run - 执行工作流

```go
func (w *WorkflowCtl) Run(ctx context.Context, concurrency int) error {
    // 1. 启动追踪 Span
    tracer := otel.Tracer("workflow-runner")
    ctx, span := tracer.Start(ctx, workflowName, ...)
    defer span.End()
    
    // 2. 更新状态为运行中
    w.mutateTask(func(task *model.WorkflowQueue) {
        task.Status = config.StatusRunning
        task.CreateTime = time.Now()
    })
    w.ack()
    
    // 3. 生成 Job 任务
    stepExecutions := GenerateJobTasks(ctx, &taskForGeneration, w.Store, w.defaultJobTimeoutSeconds)
    
    // 4. 按步骤执行
    for _, stepExec := range stepExecutions {
        priorities := sortedPriorities(stepExec.Jobs)
        for _, priority := range priorities {
            tasksInPriority := stepExec.Jobs[priority]
            
            // 确定并发度
            stepConcurrency := determineStepConcurrency(stepExec.Mode, len(tasksInPriority), seqLimit)
            stopOnFailure := !stepExec.Mode.IsParallel()
            
            // 执行该优先级的 Jobs
            job.RunJobs(ctx, tasksInPriority, stepConcurrency, w.Client, w.Store, w.ack, stopOnFailure)
            
            // 检查执行结果
            for _, task := range tasksInPriority {
                if task.Status != config.StatusCompleted {
                    w.setStatus(config.StatusFailed)
                    return fmt.Errorf("workflow %s failed at job %s", workflowName, task.Name)
                }
            }
        }
    }
    
    // 5. 标记完成
    w.updateWorkflowStatus(ctx)
    return nil
}
```

### 5.3 JobBuilder - Job 构建器

`GenerateJobTasks` 函数负责将工作流步骤转换为可执行的 Job 任务：

```go
// event/workflow/job_builder.go
func GenerateJobTasks(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore, defaultJobTimeoutSeconds int64) []StepExecution {
    // 1. 加载工作流定义
    workflow := model.Workflow{ID: task.WorkflowID}
    ds.Get(ctx, &workflow)
    
    // 2. 解析工作流步骤
    var workflowSteps model.WorkflowSteps
    json.Unmarshal(stepsBytes, &workflowSteps)
    
    // 3. 加载组件信息
    componentEntities, _ := ds.List(ctx, &model.ApplicationComponent{AppID: task.AppID}, ...)
    componentMap := make(map[string]*model.ApplicationComponent)
    for _, entity := range componentEntities {
        componentMap[component.Name] = component
    }
    
    // 4. 按步骤构建 Job
    var executions []StepExecution
    for _, step := range workflowSteps.Steps {
        mode := step.Mode
        if mode == "" {
            mode = config.WorkflowModeStepByStep
        }
        
        componentNames := step.ComponentNames()
        
        if mode.IsParallel() {
            // 并行模式：所有组件放入同一个 StepExecution
            buckets := newJobBuckets()
            appendComponentGroup(ctx, buckets, componentNames, componentMap, task, ...)
            executions = append(executions, StepExecution{Name: step.Name, Mode: mode, Jobs: buckets})
        } else {
            // 串行模式：每个组件独立成一个 StepExecution
            for _, name := range componentNames {
                buckets := newJobBuckets()
                appendComponentGroup(ctx, buckets, []string{name}, componentMap, task, ...)
                executions = append(executions, StepExecution{Name: name, Mode: config.WorkflowModeStepByStep, Jobs: buckets})
            }
        }
    }
    
    return executions
}
```

#### buildJobsForComponent - 组件转 Job

```go
func buildJobsForComponent(ctx context.Context, component *model.ApplicationComponent, task *model.WorkflowQueue, defaultJobTimeoutSeconds int64) map[int][]*model.JobTask {
    buckets := newJobBuckets()
    
    properties := ParseProperties(ctx, component.Properties)
    
    switch component.ComponentType {
    case config.ServerJob:  // webservice
        serviceJobs := job.GenerateWebService(component, &properties)
        // 附加资源（PVC、Ingress）放入高优先级
        // Deployment 本身放入普通优先级
        queueServiceJobs(logger, buckets, component, task, namespace, config.JobDeploy, serviceJobs, ...)
        
    case config.StoreJob:  // store
        storeJobs := job.GenerateStoreService(component)
        queueServiceJobs(logger, buckets, component, task, namespace, config.JobDeployStore, storeJobs, ...)
        
    case config.ConfJob:  // config
        jobTask := NewJobTask(component.Name, namespace, ...)
        jobTask.JobType = string(config.JobDeployConfigMap)
        jobTask.JobInfo = job.GenerateConfigMap(component, &properties)
        buckets[config.JobPriorityMaxHigh] = append(buckets[config.JobPriorityMaxHigh], jobTask)
        
    case config.SecretJob:  // secret
        jobTask := NewJobTask(component.Name, namespace, ...)
        jobTask.JobType = string(config.JobDeploySecret)
        jobTask.JobInfo = job.GenerateSecret(component, &properties)
        buckets[config.JobPriorityMaxHigh] = append(buckets[config.JobPriorityMaxHigh], jobTask)
    }
    
    // Service 资源（如果有端口暴露）
    if len(properties.Ports) > 0 {
        svcJob := NewJobTask(component.Name, namespace, ...)
        svcJob.JobType = string(config.JobDeployService)
        svcJob.JobInfo = job.GenerateService(component, &properties)
        buckets[config.JobPriorityNormal] = append(buckets[config.JobPriorityNormal], svcJob)
    }
    
    return buckets
}
```

`rollout` 的数据流在这个阶段收敛为原生 Kubernetes 字段：

1. `validateRolloutTrait` 负责把用户配置限制在 Eruun 支持的契约内，避免把半合法的 Kubernetes strategy 透传到 apply 阶段。
2. `workflow/traits.RolloutProcessor` 只处理顶层 workload trait，不能通过 `init` 或 `sidecar` 嵌套。
3. `webservice` 输出 `Deployment.spec.strategy`，`store` 输出 `StatefulSet.spec.updateStrategy`。
4. 删除 `traits.rollout` 后，生成的期望 workload 不再声明 strategy；后续由 Job Controller 的幂等比较决定是否需要重置线上非默认策略。

### 5.4 Job Controllers - 资源控制器

`webservice` / `store` 等 workload 组件在更新后等待目标 Pod Ready；`job` / `scheduledjob` 这类 batch Job 组件不要求 Pod 处于 Running。即时或一次性调度的 Kubernetes Job 会先以 Job `Complete` / `Failed` condition 和 `status.succeeded` 判定终态；当 Job condition 尚未同步但归属该 Job UID 的 Pod 已经 `Succeeded` 且数量达到 `spec.completions` 时，也会把 workflow JobTask 置为 `completed`。完成后控制器会先收集 Job Pod 日志，再以 UID 前置条件删除 Kubernetes Job；只有确认旧 UID 已消失，才清理仍残留且归属该 UID 的 `Succeeded` Pod，避免删除 Pod 后仍存活的 Job 补建并重复执行。

以 `DeployJobCtl` 为例说明 Job 控制器的实现：

```go
// event/workflow/job/job_deploy.go
type DeployJobCtl struct {
    namespace string
    job       *model.JobTask
    client    kubernetes.Interface
    store     datastore.DataStore
    ack       func()
}

func (c *DeployJobCtl) Run(ctx context.Context) error {
    c.job.Status = config.StatusRunning
    c.ack()  // 通知状态变更
    
    // 1. 执行资源创建/更新
    if err := c.run(ctx); err != nil {
        c.job.Error = err.Error()
        c.job.Status = config.StatusFailed
        return err
    }
    
    // 2. 等待资源就绪
    if err := c.wait(ctx); err != nil {
        c.job.Error = err.Error()
        c.job.Status = config.StatusFailed
        return err
    }
    
    c.job.Status = config.StatusCompleted
    return nil
}

func (c *DeployJobCtl) run(ctx context.Context) error {
    deploy := c.job.JobInfo.(*appsv1.Deployment)
    deployName := buildWebServiceName(c.job.Name, c.job.AppID)
    deploy.Name = deployName
    
    // 检查是否已存在
    deployLast, isAlreadyExists, err := c.deploymentExists(ctx, deployName, deploy.Namespace)
    
    if isAlreadyExists {
        if isDeploymentChanged(deployLast, deploy) {
            // 使用 Server-Side Apply 更新
            c.ApplyDeployment(ctx, deploy)
        }
        markResourceObserved(ctx, config.ResourceDeployment, deploy.Namespace, deploy.Name)
    } else {
        // 创建新资源
        c.client.AppsV1().Deployments(deploy.Namespace).Create(ctx, deploy, ...)
        MarkResourceCreated(ctx, config.ResourceDeployment, deploy.Namespace, deploy.Name)
    }
    
    return nil
}

func (c *DeployJobCtl) wait(ctx context.Context) error {
    timeout := time.After(time.Duration(c.timeout()) * time.Second)
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return NewStatusError(config.StatusCancelled, fmt.Errorf("cancelled: %w", ctx.Err()))
        case <-timeout:
            return NewStatusError(config.StatusTimeout, fmt.Errorf("timeout"))
        case <-ticker.C:
            status, _ := getDeploymentStatus(ctx, c.client, c.job.Namespace, targetName)
            if status != nil && status.Ready {
                return nil
            }
        }
    }
}

func (c *DeployJobCtl) Clean(ctx context.Context) {
    // 只清理本次创建的资源，不清理已存在的
    refs := resourcesForCleanup(ctx, config.ResourceDeployment)
    for _, ref := range refs {
        if !ref.Created {
            continue
        }
        c.client.AppsV1().Deployments(ref.Namespace).Delete(ctx, ref.Name, ...)
    }
}
```

资源控制器的幂等边界：完整 workflow 可能会再次经过数据库、Redis 等未变化组件，但核心资源 `StatefulSet`、`Service`、`PVC` 在受 Eruun 控制字段归一化后与集群当前状态等价时只记录为已观察资源，不向 Kubernetes 发送更新请求；只有 spec、labels 或 annotations 等受控字段发生实际变化时才更新。`StatefulSet` 比较不会把 `status`、`resourceVersion`、`managedFields` 或 Pod 默认 `restartPolicy`、`dnsPolicy`、`schedulerName` 等默认化差异视为变化。

### 5.5 Signal - 取消信号管理

`signal` 包实现了基于 Redis 的跨实例取消信号：

```go
// workflow/signal/cancel.go
type CancelWatcher struct {
    key      string              // eruun:workflow:cancel:<taskID>
    stopCh   chan struct{}
    state    *cancelState        // 存储取消原因
    taskID   string
    cancelFn context.CancelFunc
}

// Watch 建立取消信号监听
func Watch(ctx context.Context, taskID string) (*CancelWatcher, context.Context, context.CancelFunc, error) {
        return nil, ctx, nil, ErrCancelSignalBackendUnavailable
    }
    
    key := cancelKeyPrefix + taskID
    if err != nil && err != redis.Nil {
        return nil, ctx, nil, fmt.Errorf("inspect cancel key: %w", err)
    }
    if isCancelledToken(existing) {
        // 已取消，立即返回取消的 context
        cancelFn()
        return watcher, derivedCtx, cancelFn, nil
    }
    
    // 启动维护 goroutine
    go watcher.maintain(derivedCtx, cancelFn)
    
    return watcher, derivedCtx, cancelFn, nil
}

func (w *CancelWatcher) maintain(ctx context.Context, cancelFn context.CancelFunc) {
    ticker := time.NewTicker(cancelCheckInterval)  // 1s
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-w.stopCh:
            return
        case <-ticker.C:
            w.step(ctx, cancelFn)
        }
    }
}

func (w *CancelWatcher) step(ctx context.Context, cancelFn context.CancelFunc) {
    
    if err == redis.Nil {
        return
    }
    
    if isCancelledToken(val) {
        w.state.set(extractCancelReason(val))
        cancelFn()
        return
    }
}

// Cancel 发送取消信号
func Cancel(ctx context.Context, taskID, reason string) error {
        return ErrCancelSignalBackendUnavailable
    }
    value := "cancelled:" + reason
}
```

取消信号是 Redis 硬依赖：缺少 Redis client 时，Job 启动会失败并持久化失败状态，用户/API 取消会在修改任务状态前返回错误。取消键只存储 `cancelled:<reason>` 标记并由 TTL 清理，`Stop` 只停止当前 watcher，不删除取消标记；同一个 `taskID` 可以有多个 Job watcher 同时观察同一个取消标记。

---

## 6. 执行流程

### 6.1 任务创建与入队

用户通过 API 触发工作流执行时，系统首先在数据库中创建任务记录，状态设为 `waiting`。这确保了任务的持久化存储，即使系统重启也不会丢失。API 同步返回任务 ID，用户可以通过该 ID 查询任务状态。

```
用户请求                    WorkflowService                   数据库
   │                             │                              │
   │  POST /applications/:appID/workflow/exec                   │
   │────────────────────────────>│                              │
   │                             │                              │
   │                             │  创建 WorkflowQueue          │
   │                             │  status = waiting            │
   │                             │─────────────────────────────>│
   │                             │                              │
   │                             │<─────────────────────────────│
   │  返回 taskId                │                              │
   │<────────────────────────────│                              │
```

**关键点**：
- 任务首先写入数据库，确保持久化
- 状态初始为 `waiting`，等待被调度
- API 同步返回，用户可立即获得任务 ID
- 版本更新、直接 workflow 执行、定时调度、数据库重置、既有应用整体刷新和公共资源清理使用同一个 app-scoped 分布式锁完成“检查空闲/pending 状态并执行后续写入”；锁会在长操作期间自动续期，并发请求不会同时越过检查
- 若历史 cleanup v2/v3 仍有未完成的 StatefulSet 删除或 PVC 迁移，普通直接执行、定时调度、数据库重置、既有应用刷新和公共资源清理会被部署栅栏阻止；只能通过 `/version` 的显式全量重建恢复契约继续。级联删除整个应用是显式终止操作，它在同一锁内取消活跃任务后有意跳过该恢复栅栏
- 已提前创建且 `executeAt > 0` 的普通延迟任务在到点从 `waiting` 进入 `queued` 前，会在同一应用锁内再次检查部署栅栏；cleanup v2/v3 迁移/恢复任务自身豁免，立即任务不新增分发阶段的锁依赖
- cleanup v2/v3 只有在对应 `cleanup_resources` JobInfo 与整体 WorkflowQueue task 都为 `completed` 时才解除栅栏；cleanup 已完成但后续部署失败，或 Job/Workflow 为 `passed`、`skipped` 及其他失败终态，都会继续视为 pending。恢复 task 通过持久化的 `resolvesTaskIDs` 显式引用待消解 task，并且必须完整覆盖其组件、资源身份和 VCT 计划；栅栏不依赖 API 实例本地 `create_time` 或查询返回顺序，缺失、循环或覆盖不完整的引用都会 fail-closed
- 直接执行 API 的应用锁冲突返回 `HTTP 409 / code 10031`，锁后端不可用返回 `HTTP 503 / code 10032`；命中 pending v2/v3 栅栏返回 `HTTP 400 / code 10000` 并提示先通过版本更新 API 恢复。既有活跃/取消收敛任务仍分别返回 `HTTP 409 / code 20007` 与 `20008`

### 6.2 固定队列执行要求

所有工作流任务固定先持久化到数据库，再由 Scheduler 发布到 Redis Streams 或 Kafka，由 Worker 消费执行；运行时不存在本地执行或回退开关。

如果 `msg-type=redis` 或 `msg-type=kafka` 对应依赖不可用，服务会在启动阶段直接失败。

### 6.3 队列执行流程

`msg-type=redis` 与 `msg-type=kafka` 只选择消息后端。`scheduler` 角色负责发现任务并发布到消息队列，`worker` 角色消费执行。

Scheduler 在 `waiting -> queued` 时始终生成 `runGeneration/runToken` 并发布版本 2 消息；Worker 必须用相同 ownership 认领为 `running` 后才能 ACK，后续恢复由数据库 lease 承担。缺少版本、generation 或 token 的消息会被拒绝，不进入兼容执行路径。

```
Scheduler             Database              Message Queue          Worker              K8s
    │                    │                        │                    │                  │
    │  find waiting      │                        │                    │                  │
    │───────────────────>│                        │                    │                  │
    │  CAS queued + generation/token             │                    │                  │
    │───────────────────>│                        │                    │                  │
    │  publish v2 dispatch                        │                    │                  │
    │────────────────────────────────────────────>│                    │                  │
    │                    │                        │  consume           │                  │
    │                    │                        │───────────────────>│                  │
    │                    │  CAS running + worker/lease                 │                  │
    │                    │<────────────────────────────────────────────│                  │
    │                    │                        │  ACK after claim   │                  │
    │                    │                        │<───────────────────│                  │
    │                    │                        │                    │  execute Jobs    │
    │                    │                        │                    │─────────────────>│
    │                    │  heartbeat/status      │                    │                  │
    │                    │<────────────────────────────────────────────│                  │
```

**关键点**：
- **Scheduler/Dispatcher**：轮询数据库发现 `waiting` 任务；v2 以 CAS 创建新 generation/token 后发布消息
- **消息队列**：作为任务分发通道，支持 Redis Streams 或 Kafka
- **Worker**：从消息队列消费；v2 ownership CAS 成功后 ACK，并每 10 秒续租
- **故障恢复**：未认领消息依赖 AutoClaim/Rebalance；已 ACK 的 v2 任务由 30 秒数据库租约和 10 秒 reaper 恢复
- **延迟 Instant Job**：精确 `executionKey/runGeneration` 的 JobInfo 进入 `distributed` 后即形成持久化执行承诺；父 Workflow 后续 generation rollover 不得丢弃该延迟消息。只有尚未承诺的消息继续按当前 generation/token fencing

**为什么 Dispatcher 要轮询数据库？**
- 任务通过 API 创建，首先写入数据库（保证持久化）
- 消息队列作为"分发通道"而非"任务存储"
- 数据库支持状态查询、取消操作、重启恢复等场景

### 6.4 Step 与 Priority 执行顺序

工作流由多个 Step 组成，每个 Step 包含一组组件。Step 之间按顺序执行，Step 内部根据执行模式（StepByStep 或 DAG）决定组件的执行方式。同时，每个组件生成的 Job 按优先级分组执行，确保依赖资源（如 ConfigMap、Secret）优先创建。

创建应用时如果没有显式提供 `workflow`，服务端会按组件类型生成默认阶段化工作流：`cloudjob` -> `phase-1-job`，`config`/`secret` -> `phase-2-config-secret`，`store` -> `phase-3-store`，普通 `job` -> `phase-4-job`，`webservice`/`scheduledjob`/`service` -> `phase-5-webservice`。因此初始化 SQL 这类普通 Job 默认会排在 Store 创建之后执行；如果请求中显式提供 `workflow`，则严格按请求中的 Step 顺序执行，不再自动重排。

```
Workflow Steps 定义:
┌────────────────────────────────────────────────────────────────────┐
│  Step 1: config-step (StepByStep)     Step 2: services (DAG)      │
│  ┌─────────────────────────────┐      ┌─────────────────────────┐ │
│  │ components: [config,secret] │      │ components: [api,web]   │ │
│  └─────────────────────────────┘      └─────────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘

转换为 StepExecutions:
┌─────────────────────────────────────────────────────────────────────────────┐
│ StepExecution 1   StepExecution 2   StepExecution 3                         │
│ (config)          (secret)          (api + web)                             │
│ mode: StepByStep  mode: StepByStep  mode: DAG                              │
└─────────────────────────────────────────────────────────────────────────────┘

每个 StepExecution 内部按 Priority 执行:
┌─────────────────────────────────────────────────────────────────────────────┐
│ Priority 0 (MaxHigh)  →  Priority 1 (High)  →  Priority 10 (Normal)        │
│ [ConfigMap, Secret]      [PVC, Ingress]         [Deployment, Service]       │
└─────────────────────────────────────────────────────────────────────────────┘
```

**执行模式说明**：

| 模式 | 标识 | 行为 | 适用场景 |
|------|------|------|----------|
| 串行 | `StepByStep` | 组件逐个执行，前一个完成后才执行下一个 | 有依赖关系的组件 |
| 并行 | `DAG` | 同一 Step 内的组件并行执行 | 无依赖关系的组件 |

**优先级说明**：

| 优先级 | 值 | 资源类型 | 原因 |
|--------|-----|----------|------|
| MaxHigh | 0 | ConfigMap, Secret | 被其他资源引用，必须先创建 |
| High | 1 | PVC, ServiceAccount, Role | Deployment/StatefulSet 可能依赖 |
| Normal | 10 | Deployment, StatefulSet, Service | 主要工作负载 |
| Low | 20 | 清理任务、通知任务 | 最后执行 |

**执行顺序总结**：
1. Step 按声明顺序依次执行
2. StepByStep 模式下，组件逐个执行
3. DAG 模式下，组件并行执行
4. 每个组件的 Job 按优先级分组，高优先级先执行
5. 同一优先级的 Job 根据并发配置执行

### 6.5 并发配置对执行的影响

手工执行、到期 schedule、版本更新自动执行和数据库重置共用同一把 per-App 分布式锁，并在锁内的 datastore transaction 中完成 idle 检查和 queue task insert。同一 App 只能有一个任务生产入口越过该边界；HTTP 请求锁冲突返回 409，到期 schedule 则保留当前 `next_run` 并延后到后续轮询。锁只覆盖任务创建事务，不覆盖异步 workflow 的实际执行；不同 App 仍可并行创建和执行任务。

工作流引擎提供 `SequentialMaxConcurrency` 配置，控制 StepByStep 模式下同一优先级内 Job 的最大并发数。

**配置参数**：

| 参数 | 命令行 | 默认值 | 说明 |
|------|--------|--------|------|
| `SequentialMaxConcurrency` | `--workflow-sequential-max-concurrency` | 1 | StepByStep 模式下同优先级 Job 的最大并发数 |

**并发计算规则**：

| 执行模式 | 并发度计算 | 说明 |
|---------|-----------|------|
| DAG（并行） | `并发数 = Job 总数` | 忽略并发配置，所有 Job 全部并行 |
| StepByStep（串行） | `并发数 = min(Job 数量, SequentialMaxConcurrency)` | 受配置限制 |

**重要约束**：无论并发设置多大，**优先级是硬边界**。高优先级的所有 Job 必须全部完成后，才会开始执行低优先级的 Job。

#### 示例 1：StepByStep 模式，SequentialMaxConcurrency=1（默认）

场景：一个应用包含 3 个 config 组件和 2 个 webservice 组件，使用 StepByStep 模式部署。

**创建应用请求 (POST /applications)：**

```json
{
  "name": "demo-app",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "StepByStep 模式示例应用",
  "component": [
    {
      "name": "app-config-1",
      "type": "config",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "conf": {
          "database.host": "mysql.default.svc",
          "database.port": "3306"
        }
      },
      "traits": {}
    },
    {
      "name": "app-config-2",
      "type": "config",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "conf": {
          "redis.host": "redis.default.svc",
          "redis.port": "6379"
        }
      },
      "traits": {}
    },
    {
      "name": "app-config-3",
      "type": "config",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "conf": {
          "log.level": "info",
          "log.format": "json"
        }
      },
      "traits": {}
    },
    {
      "name": "backend",
      "type": "webservice",
      "image": "myregistry/backend:v1.0.0",
      "namespace": "default",
      "replicas": 2,
      "properties": {
        "ports": [{"port": 8080, "expose": true}],
        "env": {
          "APP_ENV": "production"
        }
      },
      "traits": {}
    },
    {
      "name": "frontend",
      "type": "webservice",
      "image": "myregistry/frontend:v1.0.0",
      "namespace": "default",
      "replicas": 2,
      "properties": {
        "ports": [{"port": 80, "expose": true}],
        "env": {
          "API_URL": "http://backend:8080"
        }
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "deploy-all",
      "mode": "StepByStep",
      "components": ["app-config-1", "app-config-2", "app-config-3", "backend", "frontend"]
    }
  ]
}
```

**执行结果：** 3 个 ConfigMap Job（Priority 0）和 2 个 Deployment Job（Priority 10）

```
时间轴 ───────────────────────────────────────────────────────────────────────────>

Priority 0 (ConfigMap):
  [ConfigMap-1] ─完成─> [ConfigMap-2] ─完成─> [ConfigMap-3] ─完成─┐
                                                                  │
                                                         等待全部完成
                                                                  │
Priority 10 (Deployment):                                         ↓
  [Deployment-1] ─完成─> [Deployment-2] ─完成─> 结束
```

执行过程：
1. Priority 0 的 3 个 ConfigMap Job **逐个串行**执行
2. 全部完成后，才开始 Priority 10
3. Priority 10 的 2 个 Deployment Job **逐个串行**执行

#### 示例 2：StepByStep 模式，SequentialMaxConcurrency=2

同样场景，但设置 `--workflow-sequential-max-concurrency=2`

```
时间轴 ───────────────────────────────────────────────────────────────────────────>

Priority 0 (ConfigMap):
  [ConfigMap-1] ──────┐
                      ├─完成─> [ConfigMap-3] ─完成─┐
  [ConfigMap-2] ──────┘                           │
                                                  │
                                         等待全部完成
                                                  │
Priority 10 (Deployment):                         ↓
  [Deployment-1] ──────┐
                       ├─完成─> 结束
  [Deployment-2] ──────┘
```

执行过程：
1. Priority 0：前 2 个 ConfigMap Job **并行**执行，完成后执行第 3 个
2. Priority 0 全部完成后，才开始 Priority 10
3. Priority 10：2 个 Deployment Job **并行**执行（因为 Job 数 ≤ 并发配置）

#### 示例 3：DAG 模式（忽略并发配置）

场景：DAG 模式下同一 Step 的所有组件并行执行

```
时间轴 ───────────────────────────────────────────────────────────────────────────>

Priority 0 (ConfigMap):
  [ConfigMap-1] ──────┐
  [ConfigMap-2] ──────┼─全部完成─┐
  [ConfigMap-3] ──────┘          │
                                 │
                        等待全部完成
                                 │
Priority 10 (Deployment):        ↓
  [Deployment-1] ──────┐
                       ├─全部完成─> 结束
  [Deployment-2] ──────┘
```

执行过程：
1. Priority 0：所有 ConfigMap Job **全部并行**执行（忽略 SequentialMaxConcurrency）
2. Priority 0 全部完成后，才开始 Priority 10
3. Priority 10：所有 Deployment Job **全部并行**执行

#### 示例 4：多 Step 组合执行

场景：2 个 Step，Step1 是 StepByStep（config 组件），Step2 是 DAG（api + web 组件）

**创建应用请求 (POST /applications)：**

```json
{
  "name": "multi-step-app",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "多 Step 组合执行示例",
  "component": [
    {
      "name": "config",
      "type": "config",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "conf": {
          "app.name": "multi-step-app",
          "app.env": "production"
        }
      },
      "traits": {}
    },
    {
      "name": "api",
      "type": "webservice",
      "image": "myregistry/api:v1.0.0",
      "namespace": "default",
      "replicas": 2,
      "properties": {
        "ports": [{"port": 8080, "expose": true}],
        "env": {
          "SERVICE_NAME": "api"
        }
      },
      "traits": {}
    },
    {
      "name": "web",
      "type": "webservice",
      "image": "myregistry/web:v1.0.0",
      "namespace": "default",
      "replicas": 2,
      "properties": {
        "ports": [{"port": 80, "expose": true}],
        "env": {
          "API_URL": "http://api:8080"
        }
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "config-step",
      "mode": "StepByStep",
      "components": ["config"]
    },
    {
      "name": "services",
      "mode": "DAG",
      "components": ["api", "web"]
    }
  ]
}
```

**执行结果：**
- Step 1 生成: ConfigMap(Priority 0)
- Step 2 生成: Deployment-api(P10) + Service-api(P10) + Deployment-web(P10) + Service-web(P10)

执行流程（SequentialMaxConcurrency=2）：

```
时间轴 ───────────────────────────────────────────────────────────────────────────>

═══ Step 1: config-step (StepByStep) ═════════════════════════════════════════════

  Priority 0:  [ConfigMap-config] ─完成─┐
                                        │
═══ Step 2: services (DAG) ═════════════╪═════════════════════════════════════════
                                        ↓
  Priority 10: [Deployment-api] ────────┐
               [Deployment-web] ────────┼─全部完成─┐
               [Service-api] ───────────┤          │
               [Service-web] ───────────┘          ↓
                                                 结束
```

执行过程：
1. **Step 1** 执行完成后，才开始 **Step 2**
2. Step 2 是 DAG 模式，同优先级的所有 Job 并行执行

### 6.6 并发控制层级架构

工作流引擎的并发控制分为两个层级：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│          第一层：MaxConcurrentWorkflows（示例 10，默认 100）                    │
│                        控制同时运行的工作流数量                                │
│                                                                             │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐        ┌───────────┐           │
│  │Workflow 1 │ │Workflow 2 │ │Workflow 3 │  ...   │Workflow 10│           │
│  │(goroutine)│ │(goroutine)│ │(goroutine)│        │(goroutine)│           │
│  │           │ │           │ │           │        │           │           │
│  │ ┌───────┐ │ │ ┌───────┐ │ │ ┌───────┐ │        │ ┌───────┐ │           │
│  │ │Job    │ │ │ │Job    │ │ │ │Job    │ │        │ │Job    │ │           │
│  │ │Pool   │ │ │ │Pool   │ │ │ │Pool   │ │        │ │Pool   │ │           │
│  │ │       │ │ │ │       │ │ │ │       │ │        │ │       │ │           │
│  │ │ ┌───┐ │ │ │ │ ┌───┐ │ │ │ │ ┌───┐ │ │        │ │ ┌───┐ │ │           │
│  │ │ │W1 │ │ │ │ │ │W1 │ │ │ │ │ │W1 │ │ │        │ │ │W1 │ │ │           │
│  │ │ │W2 │ │ │ │ │ │W2 │ │ │ │ │ │W2 │ │ │        │ │ │W2 │ │ │           │
│  │ │ │W3 │ │ │ │ │ │W3 │ │ │ │ │ │...│ │ │        │ │ │...│ │ │           │
│  │ │ └───┘ │ │ │ │ └───┘ │ │ │ │ └───┘ │ │        │ │ └───┘ │ │           │
│  │ └───────┘ │ │ └───────┘ │ │ └───────┘ │        │ └───────┘ │           │
│  └───────────┘ └───────────┘ └───────────┘        └───────────┘           │
│                                                                             │
│              第二层：Job Pool Worker（由执行模式决定）                         │
│              DAG 模式：Worker 数 = Job 数（无限制）                           │
│              StepByStep 模式：Worker 数 = SequentialMaxConcurrency           │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 并发控制参数说明

| 参数 | 默认值 | 作用层级 | 说明 |
|------|--------|----------|------|
| `MaxConcurrentWorkflows` | 100 | 工作流层 | 每个实际消费 worker 进程同时运行的工作流数量上限 |
| `SequentialMaxConcurrency` | 1 | Job Pool 层 | StepByStep 模式下 Job 并发数 |
| DAG 模式 | 无限制 | Job Pool 层 | 同优先级 Job 全部并行 |

#### 最大并行 Job 数计算公式

```
单个 worker 最大并行 Job 数 = min(请求数, MaxConcurrentWorkflows) × 每个工作流的同优先级并行 Job 数

集群最大并行 Job 数 = min(请求数, worker 进程数 × MaxConcurrentWorkflows) × 每个工作流的同优先级并行 Job 数
```

`MaxConcurrentWorkflows` 是每个实际消费进程的本地上限，不是集群级分布式信号量。每个 `worker`（以及组合模式中的 `all`）进程独立执行该上限，集群总并发可能达到 `worker 进程数 × MaxConcurrentWorkflows`。

进程关闭时先停止 Worker intake，已启动 workflow 使用独立 execution context 排空，默认上限 60 秒；超过上限后取消本地执行并停止续租，由 Scheduler reaper 恢复。Controller 或 Scheduler Leader 切换不会停止独立 Worker。

#### 不同场景下的并行 Job 数示例

| 场景 | 并发请求 | 执行模式 | 每工作流 Job 数 | 最大并行 Job |
|------|---------|---------|----------------|-------------|
| 场景 A | 5 | StepByStep（并发=1） | 3 | 5 × 1 = **5** |
| 场景 B | 5 | DAG | 3 | 5 × 3 = **15** |
| 场景 C | 10 | DAG | 3 | 10 × 3 = **30** |
| 场景 D | 15 | DAG | 3 | 10 × 3 = **30**（5个排队） |
| 场景 E | 10 | StepByStep（并发=2） | 4 | 10 × 2 = **20** |

**说明**：
- 上述场景均按单个 worker 计算；多 worker 集群按集群公式计算
- 示例场景显式按 `MaxConcurrentWorkflows=10` 计算；当前默认值为 100
- 实际执行时，还需考虑 Priority 分组，只有同优先级的 Job 才会真正并行
- 每个 webservice 组件可能生成多个 Job（Deployment + Service + PVC/Ingress 等）

#### 代码实现

工作流级别使用信号量控制：

```go
// workflow.go
type Workflow struct {
    workflowLimiter *semaphore.Weighted  // 并发限制器
}

func (w *Workflow) workerConcurrencyLimiter() *semaphore.Weighted {
    w.workerLimiterOnce.Do(func() {
        if max := w.maxWorkflowConcurrency(); max > 0 {
            w.workflowLimiter = semaphore.NewWeighted(max) // 默认 100
        }
    })
    return w.workflowLimiter
}
```

Job Pool 使用 worker 协程池：

```go
// job/job.go
func (p *Pool) Run() {
    for i := 0; i < p.concurrency; i++ {
        go p.work()  // 启动 concurrency 个 worker goroutine
    }
    // 分发任务到 worker
    for _, task := range p.Jobs {
        p.jobsChan <- task
    }
}
```

### 6.7 并发配置建议

| 场景 | 建议值 | 原因 |
|------|--------|------|
| 开发/测试环境 | 1 | 便于调试，日志清晰 |
| 小规模生产 | 2-4 | 平衡执行速度和资源消耗 |
| 大规模部署 | 4-8 | 充分利用 K8s API Server 能力 |
| 资源受限环境 | 1-2 | 避免 API Server 过载 |

**注意事项**：
- 并发数过高可能导致 K8s API Server 压力过大
- 即使设置高并发，Job 的执行仍受 K8s 调度器和资源限制影响
- 建议根据集群规模和 API Server 性能调整
- DAG 模式会显著放大并行度，生产环境需谨慎评估

---

## 7. 分布式支持

### 7.1 Redis Streams 消费者组

工作流引擎使用 Redis Streams 实现分布式任务分发：

```go
// infrastructure/messaging/redis_streams.go
type RedisStreams struct {
    key    string           // eruun.workflow.dispatch
    maxLen int64            // 流长度限制
}

// 入队
func (r *RedisStreams) Enqueue(ctx context.Context, payload []byte) (string, error) {
    args := &redis.XAddArgs{
        Stream: r.key,
        Values: map[string]interface{}{"p": payload},
    }
    if r.maxLen > 0 {
        args.MaxLen = r.maxLen  // MAXLEN 限制流长度
    }
}

// 消费
func (r *RedisStreams) ReadGroup(ctx context.Context, group, consumer string, count int, block time.Duration) ([]Message, error) {
        Group:    group,
        Consumer: consumer,
        Streams:  []string{r.key, ">"},  // > 表示只读取新消息
        Count:    int64(count),
        Block:    block,
        NoAck:    false,
    }).Result()
    // ...
}
```

### 7.2 AutoClaim 消息恢复

当 Worker 崩溃后，未确认的消息可以被其他 Worker 认领：

```go
// event/workflow/dispatcher.go
func (w *Workflow) StartWorker(consumerCtx, executionCtx context.Context, errChan chan error) {
    staleTicker := time.NewTicker(w.workerStaleInterval())  // 15s
    
    for {
        select {
        case <-staleTicker.C:
            // 定期检查并认领过期消息
            mags, err := w.Queue.AutoClaim(consumerCtx, group, consumer,
                w.workerAutoClaimMinIdle(),  // 60s
                w.workerAutoClaimCount())    // 50
            
            for _, m := range mags {
                if ack, taskID := w.processDispatchMessage(consumerCtx, workerRun, m); ack {
                    acknowledgements = append(acknowledgements, dispatchAck{id: m.ID, taskID: taskID})
                }
            }
            w.ackDispatchMessages(consumerCtx, group, consumer, acknowledgements)
            
        default:
            // 正常读取新消息
            mags, err := w.Queue.ReadGroup(consumerCtx, group, consumer,
                w.workerReadCount(),   // 10
                w.workerReadBlock())   // 2s
            // ...
        }
    }
}
```

### 7.3 指数退避重试

Worker 在遇到错误时使用指数退避策略：

```go
func (w *Workflow) StartWorker(consumerCtx, executionCtx context.Context, errChan chan error) {
    backoffMin := w.workerBackoffMin()   // 200ms
    backoffMax := w.workerBackoffMax()   // 5min
    currentDelay := backoffMin
    readFailures := 0
    
    for {
        mags, err := w.Queue.ReadGroup(consumerCtx, ...)
        if err != nil {
            readFailures++
            klog.Warningf("read group error (consecutive: %d): %v", readFailures, err)
            
            // 计算退避时间
            wait := w.workerBackoffDelay(currentDelay, backoffMin, backoffMax)
            currentDelay = wait
            
            select {
            case <-consumerCtx.Done():
                return
            case <-time.After(wait):
            }
            
            // 检查是否达到最大失败次数
            if maxReadFailures > 0 && readFailures >= maxReadFailures {
                klog.Errorf("max read failures reached (%d), worker exiting", maxReadFailures)
                return
            }
            continue
        }
        
        // 成功后重置
        readFailures = 0
        currentDelay = backoffMin
        // ...
    }
}

func (w *Workflow) workerBackoffDelay(current, min, max time.Duration) time.Duration {
    if current < min {
        return min
    }
    next := current * 2  // 指数增长
    if next > max {
        return max
    }
    return next
}
```

---

## 8. 取消与清理机制

### 8.1 取消信号流程

单任务取消接受 active 状态（包括等待、排队、运行和审批暂停）；已经是 `cancelled` 但仍存在 active Job 的任务允许重新发送取消信号。completed、failed、timeout、reject 等历史终态返回 `409 / 20013` 且保持不变。取消写入会按最新状态最多执行 3 次 CAS：active 状态间发生迁移时重新读取后重试，持续竞争则返回可重试的 `409 / 20014`。批量取消会跳过竞争期间已进入历史终态的任务，并继续处理后续任务。

Controller 的进度和终态 ACK 同样遵循状态 CAS。CAS 返回 false 后会重新读取 DB：四个持久化字段已经等于目标值时视为 MySQL no-op 成功；DB 状态已经变化时立即停止当前 Run 并保留权威状态；状态无法确认时停止当前 controller 且不发送终态回调。Worker 此时继续持有 task lease 和 workflow 并发槽，按 worker backoff 重读 DB；权威状态为 `queued` 或 `running` 时用新 controller 从持久化 checkpoint 恢复，终态或审批暂停等非当前 runner 所有的状态则停止本地恢复。数据库持续不可用时一直重试到运行时 context 取消；进程退出后由 Scheduler lease reaper 将 ownership 仍匹配的过期任务恢复为 `waiting`。因此完成与取消并发时只能有一方完成合法迁移，短暂数据库故障也不会让已 ACK 的 dispatch 永久丢失。

`cleanup_all` 失败清理继承当前 Run context。清理 Job 的 ACK 如果发现外部取消或无法确认 DB 状态，会同步取消 cleanup context；Controller 在清理返回后、写入本地失败终态前再次检查权威状态。外部 `cancelled` 获胜时不再删除后续 Kubernetes 资源，也不会把本地快照改回 `failed` 或发送 failure callback。

```
用户请求                    WorkflowService               Redis                   Worker
   │                             │                          │                        │
   │ POST /applications/:appID/workflow/cancel              │                        │
   │────────────────────────────>│                          │                        │
   │   taskId, reason            │                          │                        │
   │                             │                          │                        │
   │                             │  校验 Redis cancel backend                         │
   │                             │─────────────────────────>│                        │
   │                             │                          │                        │
   │                             │  更新 DB: cancelled      │                        │
   │                             │─────────>                │                        │
   │                             │                          │                        │
   │                             │  SET cancel:taskId       │                        │
   │                             │  "cancelled:reason"      │                        │
   │                             │─────────────────────────>│                        │
   │                             │                          │                        │
   │                             │                          │  maintain() 检测到变更  │
   │                             │                          │───────────────────────>│
   │                             │                          │                        │
   │                             │                          │  触发 ctx.Cancel()     │
   │                             │                          │<───────────────────────│
   │                             │                          │                        │
   │                             │                          │  Job.Clean() 清理资源  │
   │                             │                          │<───────────────────────│
   │                             │                          │                        │
   │  返回成功                   │                          │                        │
   │<────────────────────────────│                          │                        │
```

### 8.2 资源清理跟踪器

清理跟踪器记录每个 Job 创建的资源，便于失败时精确清理：

```go
// event/workflow/job/cleanup_tracker.go
type CleanupTracker struct {
    mu        sync.Mutex
    resources map[config.ResourceKind][]ResourceRef
}

type ResourceRef struct {
    Name      string
    Namespace string
    Created   bool  // true=本次创建, false=已存在只是观察
}

// MarkResourceCreated 标记资源为本次创建
func MarkResourceCreated(ctx context.Context, kind config.ResourceKind, namespace, name string) {
    tracker := trackerFromContext(ctx)
    if tracker == nil {
        return
    }
    tracker.mu.Lock()
    defer tracker.mu.Unlock()
    tracker.resources[kind] = append(tracker.resources[kind], ResourceRef{
        Name:      name,
        Namespace: namespace,
        Created:   true,
    })
}

// markResourceObserved 标记资源为已存在（更新场景）
func markResourceObserved(ctx context.Context, kind config.ResourceKind, namespace, name string) {
    tracker := trackerFromContext(ctx)
    if tracker == nil {
        return
    }
    tracker.mu.Lock()
    defer tracker.mu.Unlock()
    tracker.resources[kind] = append(tracker.resources[kind], ResourceRef{
        Name:      name,
        Namespace: namespace,
        Created:   false,  // 不是本次创建，清理时跳过
    })
}

// resourcesForCleanup 获取需要清理的资源
func resourcesForCleanup(ctx context.Context, kind config.ResourceKind) []ResourceRef {
    tracker := trackerFromContext(ctx)
    if tracker == nil {
        return nil
    }
    tracker.mu.Lock()
    defer tracker.mu.Unlock()
    return tracker.resources[kind]
}
```

### 8.3 Job 清理实现

```go
// event/workflow/job/job.go
func runJob(ctx context.Context, job *model.JobTask, ...) {
    ctx = WithCleanupTracker(ctx)  // 注入清理跟踪器
    
    // 设置取消信号监听
    if taskID := TaskIDFromContext(ctx); taskID != "" {
        watcher, jobCtx, cancelFn, _ = signal.Watch(ctx, taskID)
    }
    
    defer func() {
        // panic 恢复时清理
        if r := recover(); r != nil {
            if !cleaned {
                jobCtl.Clean(jobCtx)
                cleaned = true
            }
            job.Status = config.StatusFailed
        }
    }()
    
    if err := jobCtl.Run(jobCtx); err != nil {
        // 执行失败时清理
        if !cleaned {
            jobCtl.Clean(jobCtx)
            cleaned = true
        }
        
        // 根据错误类型设置状态
        if errors.Is(err, context.Canceled) {
            job.Status = config.StatusCancelled
        } else {
            job.Status = config.StatusFailed
        }
    }
    
    // 失败状态也需要清理
    if !cleaned && jobStatusFailed(job.Status) {
        jobCtl.Clean(jobCtx)
    }
}
```

### 8.4 Workflow 失败清理策略

Workflow steps JSON 支持 workflow 级 `failurePolicy` 字段。旧数组写法、空值和历史缺字段数据默认都是 `cleanup_all`：

| 策略 | 触发条件 | 清理范围 |
|------|----------|----------|
| `cleanup_all` | 部署类 Job `failed` 或 `timeout` | Controller 复用 `cleanup_resources` job，为当前 App 下全部 DB 已知组件逐个清理普通 Kubernetes 运行资源；standalone PVC 和五类 RBAC 保留 |
| `cleanup_failed` | 单个 Job 执行失败或超时 | 显式 opt-out 策略。沿用 `runJob -> jobCtl.Clean`，只清理失败 job 自己负责且已创建的普通运行资源；standalone PVC 和五类 RBAC 保留 |

执行 `cleanup_all` 时，Controller 会保持串行顺序尝试全部 cleanup jobs；单个 cleanup job 失败不会阻止后续组件清理，多个 cleanup 失败会聚合到 workflow 终态原因中。

`cleanup_all` 不删除 App、Workflow、Component DB 实体，也不扩展人工取消、审批拒绝、callback 失败或 `cleanup_resources` 自身失败的语义。普通共享资源保护仍由现有 `cleanup_resources` 规则负责；ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding 不依赖 share 标签，任何 cleanup 路径都不会删除。

需要为整条 workflow 保留旧的只清失败 job 行为时，在 workflow 上显式设置 `failurePolicy: cleanup_failed`。只需要让单个 `type=job` 的主 Instant Job 退出 workflow 默认 `cleanup_all` 时，可在该组件的 `properties` 中设置 `failurePolicy: cleanup_failed`；该 Job override 优先于 workflow 策略，附属 PVC、RBAC 等任务仍继承 workflow 策略，但其永久保留边界不受策略影响。并行失败中只要任一任务的有效策略仍为 `cleanup_all`，Controller 就执行全量清理，并在实际触发任务不同于首个失败任务时把触发任务追加到终态原因和 callback `reason`。

---

## 9. 状态管理

### 9.1 任务状态转换

```go
// 任务状态定义
const (
    StatusWaiting   Status = "waiting"   // 等待执行
    StatusQueued    Status = "queued"    // 已入队
    StatusRunning   Status = "running"   // 执行中
    StatusCompleted Status = "completed" // 完成
    StatusFailed    Status = "failed"    // 失败
    StatusTimeout   Status = "timeout"   // 超时
    StatusCancelled Status = "cancelled" // 取消
)
```

### 9.2 状态转换规则

| 当前状态 | 可转换到 | 触发条件 |
|----------|----------|----------|
| waiting | queued | Dispatcher 获取到执行权 |
| queued | running | Worker 开始执行 |
| running | completed | 所有 Job 执行成功 |
| running | failed | 任一 Job 执行失败 |
| running | timeout | Job 执行超时 |
| running | cancelled | 收到取消信号 |
| queued | waiting | 分发失败，回滚状态 |

### 9.3 CAS 状态更新

为防止并发冲突，状态转换使用 CAS (Compare-And-Swap)：

```go
// domain/repository/workflow.go
func UpdateTaskStatus(ctx context.Context, store datastore.DataStore, taskID string, from, to config.Status) (bool, error) {
    // 使用条件更新：WHERE task_id=? AND status=?
    result := db.Model(&model.WorkflowQueue{}).
        Where("task_id = ? AND status = ?", taskID, from).
        Update("status", to)
    
    if result.RowsAffected == 0 {
        return false, nil  // 状态已被其他实例修改
    }
    return true, nil
}
```

### 9.4 ACK 回调机制

`ack` 回调确保状态变更及时持久化：

```go
// controller.go
func NewWorkflowController(workflowTask *model.WorkflowQueue, ..., urlSecurityPolicy *spec.URLSecurityPolicySpec) (*WorkflowCtl, error) {
    if urlSecurityPolicy == nil {
        return nil, fmt.Errorf("url security policy is required")
    }
    ctl := &WorkflowCtl{
        workflowTask:      workflowTask,
        urlSecurityPolicy: urlSecurityPolicy,
        // ...
    }
    ctl.ack = ctl.updateWorkflowTask  // 绑定 ACK 回调
    return ctl, nil
}

func (w *WorkflowCtl) updateWorkflowTask() {
    taskSnapshot, expectedStatus, stopped := w.snapshotTaskPersistence()
    if stopped {
        return
    }

    updates := map[string]interface{}{
        "status":                taskSnapshot.Status,
        "current_step":          taskSnapshot.CurrentStep,
        "approval_pending":      taskSnapshot.ApprovalPending,
        "pending_approval_step": taskSnapshot.PendingApprovalStep,
    }
    persistCtx := w.ctx
    if persistCtx == nil {
        persistCtx = context.Background()
    }
    authoritativeTask, err := w.persistWorkflowTaskSnapshot(
        persistCtx,
        taskSnapshot,
        expectedStatus,
        updates,
    )
    if err != nil {
        // DB 状态无法确认：停止 Run，并抑制终态回调。
        w.stopTaskPersistence(nil, true)
        return
    }
    if authoritativeTask != nil {
        // 取消或其他状态迁移已获胜：使用 DB 快照并停止 Run。
        w.stopTaskPersistence(authoritativeTask, false)
        return
    }

    w.workflowTaskMutex.Lock()
    if !w.taskPersistenceStopped && w.persistedTaskStatus == expectedStatus {
        w.persistedTaskStatus = taskSnapshot.Status
    }
    w.workflowTaskMutex.Unlock()
}
```

`persistWorkflowTaskSnapshot` 对一次不明确的 miss 最多额外重试 1 次。只有 DB 中的 `runGeneration/runToken/workerId` 仍与本次执行完全一致，且 `status`、`current_step`、`approval_pending`、`pending_approval_step` 已经与目标快照一致时，MySQL `RowsAffected` 为 0 才按成功处理。execution identity 不同即返回权威快照并取消旧 Run，防止旧 generation 因进度字段恰好相同而继续执行。真正的外部状态迁移同样会取消 Run context，使串行、并行和失败清理 Job runner 不再启动新的 Job。无法确认状态时，外层 worker 在不释放 task lease 的前提下执行上述权威快照恢复循环。

---

## 10. 并发控制

### 10.1 工作流级别并发限制

使用信号量限制同时运行的工作流数量：

```go
func (w *Workflow) StartWorker(consumerCtx, executionCtx context.Context, errChan chan error) {
    workerRun := newWorkflowWorkerRun(executionCtx, w.workerConcurrencyLimiter())
    defer workerRun.wait()
    // intake 使用 consumerCtx；已认领任务使用 executionCtx。
}

func (w *Workflow) runWorkflowTask(
    ctx context.Context,
    workerRun *workflowWorkerRun,
    task *model.WorkflowQueue,
    concurrency int,
) (bool, error) {
    runnerCtx := workerRun.executionCtx
    if workerRun.limiter != nil {
        if err := workerRun.limiter.Acquire(runnerCtx, 1); err != nil {
            return false, err
        }
    }
    workerRun.taskGroup.Go(func() error {
        if workerRun.limiter != nil {
            defer workerRun.limiter.Release(1)
        }
        return w.runWorkflowControllerWithPersistenceRecovery(runnerCtx, controller, concurrency)
    })
    return true, nil
}
```

### 10.2 Step 内 Job 并发控制

```go
// controller.go
func determineStepConcurrency(mode config.WorkflowMode, jobCount, sequentialLimit int) int {
    if jobCount <= 0 {
        return 0
    }
    if mode.IsParallel() {
        return jobCount  // 并行模式：全部并发执行
    }
    // 串行模式：受 sequentialLimit 限制
    if sequentialLimit < 1 {
        sequentialLimit = 1
    }
    if jobCount < sequentialLimit {
        return jobCount
    }
    return sequentialLimit
}
```

### 10.3 Job Pool 并发执行

```go
// job/job.go
type Pool struct {
    Jobs          []*model.JobTask
    concurrency   int
    jobsChan      chan *model.JobTask
    stopOnFailure bool
    wg            sync.WaitGroup
    failureOnce   sync.Once
}

func (p *Pool) Run() {
    defer p.cancel()
    
    // 启动 worker goroutines
    for i := 0; i < p.concurrency; i++ {
        go p.work()
    }
    
    // 分发任务
    for _, task := range p.Jobs {
        if p.stopOnFailure && p.ctx.Err() != nil {
            break  // 停止分发
        }
        p.wg.Add(1)
        p.jobsChan <- task
    }
    
    close(p.jobsChan)
    p.wg.Wait()
}

func (p *Pool) work() {
    for job := range p.jobsChan {
        runJob(p.ctx, job, p.client, p.store, p.ack)
        
        if p.stopOnFailure && jobStatusFailed(job.Status) {
            p.failureOnce.Do(func() {
                p.cancel()  // 通知其他 worker 停止
            })
        }
        p.wg.Done()
    }
}
```

---

## 11. 配置参考

### 11.1 WorkflowRuntimeConfig

```go
// config/config.go
type WorkflowRuntimeConfig struct {
    // 串行步骤内部最大并发数（默认 1）
    SequentialMaxConcurrency int
    
    // Dispatcher 扫描间隔（默认 3s）
    DispatchPollInterval time.Duration
    
    // Worker 过期检查间隔（默认 15s）
    WorkerStaleInterval time.Duration
    
    // AutoClaim 最小空闲时间（默认 60s）
    WorkerAutoClaimMinIdle time.Duration
    
    // AutoClaim 批量大小（默认 50）
    WorkerAutoClaimCount int
    
    // Worker 单次读取消息数（默认 10）
    WorkerReadCount int
    
    // Worker 阻塞读取超时（默认 2s）
    WorkerReadBlock time.Duration
    
    // Job 默认超时时间（默认 60s）
    DefaultJobTimeout time.Duration
    
    // 最大并发工作流数（每个 Worker 默认 100）
    MaxConcurrentWorkflows int

    // Worker 心跳间隔（默认 10s）
    HeartbeatInterval time.Duration

    // Workflow 数据库租约（默认 30s）
    LeaseDuration time.Duration

    // Scheduler 租约回收扫描间隔（默认 10s）
    LeaseReaperInterval time.Duration

    // Worker 优雅排空上限（默认 60s）
    WorkerDrainTimeout time.Duration
    
    // Worker 最大连续读取失败次数（0=无限重试）
    WorkerMaxReadFailures int
    
    // Worker 最大连续认领失败次数（0=无限重试）
    WorkerMaxClaimFailures int
    
    // 退避最小时间（默认 200ms）
    WorkerBackoffMin time.Duration
    
    // 退避最大时间（默认 5min）
    WorkerBackoffMax time.Duration
}
```

说明：`WorkerMaxReadFailures`、`WorkerMaxClaimFailures`、`WorkerBackoffMin` 与 `WorkerBackoffMax` 是当前运行时内部配置字段，`Config.NewConfig()` 会设置默认值；当前服务端命令行未暴露对应 `--workflow-worker-*failure/backoff*` 参数。

### 11.2 MessagingConfig

```go
// config/config.go
type MessagingConfig struct {
    // 消息队列类型：redis | kafka
    Type          string
    
    // 消息通道/Topic 前缀
    ChannelPrefix string
    
    // === Redis 配置 ===
    // Redis Stream 最大长度，<=0 表示不限制
    RedisStreamMaxLen int64
    
    // === Kafka 配置 ===
    // Kafka Broker 地址列表
    KafkaBrokers []string
    
    // Kafka 消费者组 ID（默认: eruun-workflow-workers）
    KafkaGroupID string
    
    // Kafka 偏移量重置策略: earliest | latest（默认: earliest）
    KafkaAutoOffsetReset string
}
```

### 11.3 命令行参数详解

#### 工作流参数

| 参数 | 默认值 | 说明 | 推荐配置 |
|------|--------|------|----------|
| `--workflow-sequential-max-concurrency` | 1 | 串行步骤内部最大并发数 | 生产环境建议 1-3 |
| `--workflow-dispatch-poll-interval` | 3s | Dispatcher 扫描间隔 | 生产环境建议 3-5s |
| `--workflow-worker-stale-interval` | 15s | Worker 过期检查间隔 | 生产环境建议 15-30s |
| `--role` | api | 运行角色：api/controller/scheduler/worker | 每个进程只运行一类职责 |
| `--controller-lock-name` | eruun-controller | Controller Leader Lease | 同 namespace 内唯一 |
| `--scheduler-lock-name` | eruun-scheduler | Scheduler Leader Lease | 不得与 Controller 相同 |
| `--workflow-heartbeat-interval` | 10s | Worker DB 心跳 | 小于 lease duration |
| `--workflow-lease-duration` | 30s | DB 执行租约 | 必须大于 heartbeat |
| `--workflow-lease-reaper-interval` | 10s | 过期租约扫描 | 默认保持 60 秒 RTO |
| `--workflow-worker-drain-timeout` | 60s | 关闭时任务排空上限 | 与恢复目标对齐 |
| `--workflow-worker-autoclaim-idle` | 60s | AutoClaim 最小空闲时间 | 应大于 Job 最大执行时间 |
| `--workflow-worker-autoclaim-count` | 50 | AutoClaim 批量大小 | 根据任务量调整 |
| `--workflow-worker-read-count` | 10 | Worker 单次读取消息数 | 根据处理能力调整 |
| `--workflow-worker-read-block` | 2s | Worker 阻塞读取超时 | 建议 2-5s |
| `--workflow-default-job-timeout` | 60s | Job 默认超时时间 | 根据业务需求调整 |
| `--workflow-callback-timeout-max` | 72h | Workflow 回调超时时间上限 | 跨天审批建议保留 72h |
| `--workflow-max-concurrent` | 100 | 每个 Worker 最大并发工作流数 | 按 Worker 副本与 K8s API 容量调整 |

#### 消息队列参数

| 参数 | 默认值 | 说明 | 推荐配置 |
|------|--------|------|----------|
| `--msg-type` | redis | 消息队列类型 | redis/kafka |
| `--msg-channel-prefix` | 空（运行时按 `eruun` 生效） | 消息通道前缀 | 根据环境区分，如 eruun-prod |
| `--msg-redis-maxlen` | 50000 | Redis Stream 最大长度 | 根据消息量和内存调整 |

#### Kafka 参数

| 参数 | 默认值 | 说明 | 推荐配置 |
|------|--------|------|----------|
| `--msg-kafka-brokers` | - | Kafka Broker 地址 | 生产环境建议配置多个 |
| `--msg-kafka-group-id` | eruun-workflow-workers | 消费者组 ID | 不同环境使用不同 ID |
| `--msg-kafka-offset-reset` | earliest | 偏移量重置策略 | 生产环境建议 earliest |

#### 日志参数

| 参数 | 默认值 | 说明 | 推荐配置 |
|------|--------|------|----------|
| `--log_dir` | 空 | 日志输出目录，启用后会写入日志文件并启动日志清理服务（默认保留 7 天） | 部署默认 `./logs`（工作目录下） |
| `--logtostderr` | true | 仅输出到 stderr；为写入日志目录需设为 false | 部署默认 `false` |
| `--alsologtostderr` | false | 同时输出到 stderr，便于 `kubectl logs` 查看 | 部署默认 `true` |

### 11.4 命令行参数示例

```bash
# 工作流相关参数
--workflow-sequential-max-concurrency=3     # 串行步骤并发数
--workflow-dispatch-poll-interval=3s        # Dispatcher 扫描间隔
--workflow-worker-stale-interval=15s        # 过期检查间隔
--workflow-worker-autoclaim-idle=60s        # AutoClaim 空闲时间
--workflow-worker-autoclaim-count=50        # AutoClaim 批量大小
--workflow-worker-read-count=10             # 单次读取数
--workflow-worker-read-block=2s             # 阻塞读取超时
--workflow-default-job-timeout=60s          # Job 默认超时
--workflow-callback-timeout-max=72h         # 回调超时时间上限
--workflow-max-concurrent=100               # 每个 Worker 最大并发工作流数

# 消息队列相关参数
--msg-type=redis                            # 队列类型：redis|kafka
--msg-channel-prefix=eruun                # 消息通道前缀
--msg-redis-maxlen=50000                    # Redis Stream 最大长度

# Kafka 相关参数（当 msg-type=kafka 时使用）
--msg-kafka-brokers=localhost:9092          # Kafka broker 地址列表
--msg-kafka-group-id=eruun-workflow       # Kafka 消费者组 ID
--msg-kafka-offset-reset=earliest           # 偏移量重置策略：earliest|latest

# 日志相关参数
--log_dir=./logs                            # 日志目录（工作目录下）
--logtostderr=false                         # 写文件需关闭 logtostderr
--alsologtostderr=true                      # 同时输出到 stderr
```

### 11.5 配置示例

#### 开发环境配置（Redis 模式）

```yaml
# config-dev.yaml - 开发环境使用本地 Redis，保持与生产一致的调度路径
workflow:
  sequentialMaxConcurrency: 1
  dispatchPollInterval: 1s
  workerStaleInterval: 10s
  workerAutoClaimMinIdle: 30s
  workerAutoClaimCount: 10
  workerReadCount: 5
  workerReadBlock: 1s
  defaultJobTimeout: 30s
  maxConcurrentWorkflows: 5
  workerMaxReadFailures: 0
  workerMaxClaimFailures: 0
  workerBackoffMin: 100ms
  workerBackoffMax: 1m

messaging:
  type: redis
  channelPrefix: eruun-dev
```

#### 生产环境配置（Redis 模式）

```yaml
# config-prod-redis.yaml - 中等规模生产环境推荐配置
workflow:
  sequentialMaxConcurrency: 3
  dispatchPollInterval: 3s
  workerStaleInterval: 15s
  workerAutoClaimMinIdle: 60s    # 确保大于 defaultJobTimeout
  workerAutoClaimCount: 50
  workerReadCount: 10
  workerReadBlock: 2s
  defaultJobTimeout: 60s
  maxConcurrentWorkflows: 20     # 根据 K8s API 承载能力调整
  workerMaxReadFailures: 0       # 无限重试，增强弹性
  workerMaxClaimFailures: 0
  workerBackoffMin: 200ms
  workerBackoffMax: 5m

messaging:
  type: redis
  channelPrefix: eruun-prod
  redisStreamMaxLen: 100000      # 根据内存和消息量调整

# Redis 连接配置（复用 Cache 配置）
cache:
  cacheHost: redis.prod.svc.cluster.local
  cacheProt: 6379
  cacheType: redis
  cacheDB: 0
  cacheTTL: 24h
  keyPrefix: "eruun:cache:"
```

#### 大规模生产环境配置（Kafka 模式）

```yaml
# config-prod-kafka.yaml - 大规模生产环境推荐配置
workflow:
  sequentialMaxConcurrency: 5
  dispatchPollInterval: 3s
  workerStaleInterval: 20s
  workerAutoClaimMinIdle: 120s   # Kafka rebalance 可能需要更长时间
  workerAutoClaimCount: 100
  workerReadCount: 20            # Kafka 吞吐量高，可增加读取数
  workerReadBlock: 3s
  defaultJobTimeout: 120s
  maxConcurrentWorkflows: 50     # 大规模部署
  workerMaxReadFailures: 0
  workerMaxClaimFailures: 0
  workerBackoffMin: 200ms
  workerBackoffMax: 5m

messaging:
  type: kafka
  channelPrefix: eruun-prod
  kafkaBrokers:
    - kafka-0.kafka.prod.svc.cluster.local:9092
    - kafka-1.kafka.prod.svc.cluster.local:9092
    - kafka-2.kafka.prod.svc.cluster.local:9092
  kafkaGroupID: eruun-workflow-workers
  kafkaAutoOffsetReset: earliest
```

### 11.6 配置调优建议

#### 性能调优

| 场景 | 推荐配置 | 说明 |
|------|----------|------|
| 高吞吐量 | `workerReadCount=20`, `maxConcurrentWorkflows=50` | 增加并发处理能力 |
| 低延迟 | `dispatchPollInterval=1s`, `workerReadBlock=1s` | 减少轮询间隔 |
| 资源受限 | `maxConcurrentWorkflows=5`, `workerReadCount=3` | 限制并发避免过载 |
| 长时任务 | `defaultJobTimeout=300s`, `workerAutoClaimMinIdle=360s` | 延长超时时间 |

#### 消息队列选型建议

| 场景 | 推荐 | 原因 |
|------|------|------|
| 开发测试 | redis | 与生产同路径，便于提前暴露依赖问题 |
| 中小规模 (<100 任务/s) | redis | 部署简单，延迟低 |
| 大规模 (>100 任务/s) | kafka | 高吞吐量，强持久化 |
| 已有 Redis 基础设施 | redis | 复用现有资源 |
| 已有 Kafka 基础设施 | kafka | 复用现有资源 |
| 需要消息回溯 | kafka | 支持历史消息重放 |

#### 关键配置约束

1. **workerAutoClaimMinIdle > defaultJobTimeout**：确保任务超时后才被认领
2. **workerStaleInterval < workerAutoClaimMinIdle**：确保定期检查过期任务
3. **Redis maxLen**：根据消息量和内存设置，防止内存溢出
4. **Kafka partitions >= workers**：确保每个 Worker 都能分配到分区

---

## 12. 优势总结

### 12.1 多后端部署灵活性

| 特性 | Redis 分布式 | Kafka 分布式 |
|------|--------------|--------------|
| 依赖 | MySQL + Redis | MySQL + Kafka |
| 部署 | 多实例 | 多实例 |
| 故障恢复 | AutoClaim 自动恢复 | Rebalance 自动恢复 |
| 适用场景 | 中小规模生产环境 | 大规模生产环境 |
| 吞吐量 | 中等 | 高 |
| 配置 | msg-type=redis | msg-type=kafka |

### 12.2 完善的可观测性

- **分布式追踪**：集成 OpenTelemetry，支持 Jaeger 等后端
- **结构化日志**：使用 klog 带 traceID、workflowName、taskID
- **状态查询**：提供 `/workflow/tasks/:taskID/status` API
- **阶段详情**：提供 `/workflow/tasks/:taskID/stages` API（按阶段名聚合同名任务，类型以列表展示，info/error 以 JSON 数组返回）
- **组件级状态**：细粒度追踪每个组件的执行状态
- **应用运行态查询**：提供 `/applications/:appID/status` API（从 app_components 聚合应用状态）
- **组件运行态查询**：提供 `/applications/:appID/components/status` API（返回 app_components 中每个组件的 Running/Pending/Failed 等运行态）

### 12.3 安全的资源清理

- **精确清理**：只清理本次创建的资源，不影响已有资源
- **幂等设计**：支持重复执行，更新场景不会误删
- **超时保护**：清理过程有 30s 超时限制
- **panic 恢复**：异常情况下也能正确清理

### 12.4 高可用的消息处理

- **消费者组**：支持多 Worker 负载均衡
- **消息恢复**：Redis 使用 AutoClaim 自动认领超时消息，Kafka 使用原生 Rebalance 机制
- **指数退避**：错误时智能重试，避免雪崩
- **弹性配置**：可配置最大失败次数，0 表示无限重试
- **多后端支持**：支持 Redis Streams 和 Apache Kafka 两种分布式队列

### 12.5 灵活的执行控制

- **优先级调度**：确保依赖资源优先创建
- **串行/并行模式**：满足不同业务场景
- **并发限制**：保护 K8s API Server 和集群资源
- **取消支持**：支持用户主动取消和超时取消

---

## 附录

### A. 关键代码文件索引

| 功能 | 文件路径 |
|------|----------|
| 工作流入口 | `pkg/apiserver/event/workflow/workflow.go` |
| 任务控制器 | `pkg/apiserver/event/workflow/controller.go` |
| 消息分发 | `pkg/apiserver/event/workflow/dispatcher.go` |
| Job 构建器 | `pkg/apiserver/event/workflow/job_builder.go` |
| Job 执行器 | `pkg/apiserver/event/workflow/job/job.go` |
| Deployment 控制器 | `pkg/apiserver/event/workflow/job/job_deploy.go` |
| StatefulSet 控制器 | `pkg/apiserver/event/workflow/job/job_statefulset.go` |
| 清理跟踪器 | `pkg/apiserver/event/workflow/job/cleanup_tracker.go` |
| 取消信号 | `pkg/apiserver/workflow/signal/cancel.go` |
| 队列接口 | `pkg/apiserver/infrastructure/messaging/queue.go` |
| Redis Streams | `pkg/apiserver/infrastructure/messaging/redis_streams.go` |
| Kafka Queue | `pkg/apiserver/infrastructure/messaging/kafka.go` |
| 数据模型 | `pkg/apiserver/domain/model/workflow.go` |
| 配置定义 | `pkg/apiserver/config/config.go` |
| 常量定义 | `pkg/apiserver/config/consts.go` |

### B. API 接口列表

以下路径均省略 `/api/v1` 前缀。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/applications/:appID/workflows` | 查询应用工作流列表 |
| PUT | `/applications/:appID/workflow` | 更新工作流 |
| POST | `/applications/:appID/workflow/exec` | 执行工作流任务 |
| POST | `/applications/:appID/workflow/cancel` | 取消工作流任务 |
| POST | `/applications/:appID/workflow/tasks/cancel-all` | 取消应用下所有活跃工作流任务 |
| POST | `/applications/convert` | 转换 Kubernetes YAML 为组件配置 |
| GET | `/workflow/tasks/:taskID/status` | 查询任务状态 |
| GET | `/workflow/tasks/:taskID/stages` | 查询任务阶段详情 |
| GET | `/applications/:appID/status` | 查询应用聚合运行态 |
| GET | `/applications/:appID/components/status` | 查询应用组件运行态明细 |

`/applications/convert` 采用 best-effort 转换契约：请求成功表示至少生成了可返回的组件配置；无法表达、缺字段、无法匹配或有损映射的 Kubernetes 资源/字段会被跳过并进入 `warnings`。响应中的 `valid` 只表示转换后的 Eruun 应用模型是否通过 `TryApplication` 校验，不代表输入 YAML 已被无损转换。

### C. 状态码定义

```go
// utils/bcode/002_workflow.go
var (
    ErrWorkflowConfig                  = NewBcode(400, 20000, "workflow config does not comply with OAM specification")
    ErrWorkflowExist                   = NewBcode(400, 20001, "workflow name is exist")
    ErrCreateWorkflow                  = NewBcode(400, 20002, "workflow create failure")
    ErrCreateComponents                = NewBcode(400, 20003, "workflow components create failure")
    ErrExecWorkflow                    = NewBcode(400, 20004, "workflow exec failure")
    ErrWorkflowNotExist                = NewBcode(404, 20005, "workflow not found")
    ErrWorkflowTaskNotExist            = NewBcode(404, 20006, "workflow task not found")
    ErrWorkflowTaskRunning             = NewBcode(409, 20007, "workflow task is running")
    ErrWorkflowTaskCancelling          = NewBcode(409, 20008, "workflow task is cancelling")
    ErrWorkflowTaskNotAwaitingApproval = NewBcode(409, 20009, "workflow task is not awaiting approval")
    ErrWorkflowApprovalActionInvalid   = NewBcode(400, 20010, "workflow approval action is invalid")
    ErrWorkflowEmpty                   = NewBcode(400, 20011, "workflow is empty, please edit workflow before executing")
    ErrWorkflowCancelSignalUnavailable = NewBcode(503, 20012, "workflow cancel signal backend is unavailable")
    ErrWorkflowTaskNotCancellable      = NewBcode(409, 20013, "workflow task cannot be cancelled in current status")
    ErrWorkflowTaskCancelConflict      = NewBcode(409, 20014, "workflow task state changed while cancelling; retry")
)
```

---

*文档版本：1.0.1*
*最后更新：2026-07*
