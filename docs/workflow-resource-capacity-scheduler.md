# Workflow 资源补偿型 Scheduler 技术方案

> 状态：Draft / Proposal。本文是容量补偿型 scheduler 方案，不代表当前运行链路已接入该 scheduler 层。

## 1. 背景

当前 Eruun 的 Workflow 执行链路已经具备持久化队列、分布式派发、Worker 消费、Workflow Controller 编排和 Job 执行能力：

```text
WorkflowQueue -> Dispatcher -> Redis/Kafka -> Worker -> WorkflowCtl -> Job
```

这条链路可以保证任务被可靠执行，但资源是否足够主要依赖 Kubernetes Scheduler 在 Pod 创建后处理。当集群资源不足时，Pod 可能长时间 Pending，用户只能在部署已经开始后才看到资源问题。

新的 Scheduler 层用于在 Workflow 部署前做资源准入和容量补偿：当集群现有资源不足时，先触发云资源创建动作，例如创建 ECS 并将其加入 Kubernetes 集群；所有部署任务等待新服务器成为 Ready Node 后，再继续执行原 Workflow。

## 2. 目标

- 在 Workflow 执行部署前，统一评估所有 Pod 型工作负载的资源需求。
- 第一版默认覆盖所有工作负载类型，包括 Deployment、StatefulSet、Job、CronJob 等。
- 资源不足时自动触发容量补偿流程，而不是直接开始部署并让 Pod Pending。
- 容量补偿复用现有 `cloudjob` provider/action 机制。
- 云服务器创建完成不视为成功，必须等待 Kubernetes 中出现满足条件的 Ready Node。
- 容量满足后，为工作负载注入软调度提示，优先使用新增节点，同时不覆盖用户已有硬约束。
- Scheduler 决策需要可观测、可重试、可恢复，并且扩容动作必须幂等。

## 3. 非目标

- 不替代 Kubernetes Scheduler，不直接绑定 Pod 到具体 Node。
- 第一版不实现完整多云弹性伸缩平台，只定义并接入容量补偿动作。
- 不新增独立调度 CRD 或复杂调度数据库实体，优先复用现有 Workflow、Job、CloudJob 机制。
- 不在资源不足时静默降级继续部署。

## 4. 总体架构

Scheduler 位于 Workflow Controller 生成 Job 计划之后、真正执行用户部署 Job 之前。

```text
WorkflowQueue
  -> Dispatcher 派发任务
  -> Worker 消费任务
  -> WorkflowCtl 加载 Workflow 并生成 Job 计划
  -> Scheduler 评估资源需求
      -> 资源充足：生成 placement hint，继续执行原 Workflow
      -> 资源不足：插入 capacity-provision 阶段
          -> CloudJob 创建 ECS / 扩容节点池
          -> 等待新 Node 注册并 Ready
          -> 生成 placement hint
  -> 执行原 Workflow Steps / Jobs
```

Scheduler 的职责是“容量保障”和“调度提示”，最终 Pod 放置仍由 Kubernetes Scheduler 决定。

## 5. 核心流程

### 5.1 资源充足流程

1. WorkflowCtl 根据 workflow steps 和 components 生成 Job 计划。
2. Scheduler 从 Job 计划中提取所有 Pod 型工作负载。
3. Scheduler 计算本次 Workflow 的资源需求。
4. Scheduler 获取当前集群 Node 与 Pod 快照，计算可用资源。
5. 如果资源足够，Scheduler 生成 placement hint。
6. WorkflowCtl 按原有 Step / Priority 顺序继续执行。

### 5.2 资源不足流程

1. Scheduler 计算出资源缺口。
2. Scheduler 根据策略生成 `capacityPlan`。
3. WorkflowCtl 在原部署阶段前执行 `capacity-provision` 阶段。
4. `capacity-provision` 通过 `cloudjob` provider/action 创建 ECS 或扩容节点池。
5. Cloud action 轮询云侧状态和 Kubernetes Node 状态。
6. 新节点注册进 Kubernetes 并处于 Ready 后，容量阶段完成。
7. Scheduler 重新评估资源或使用新增节点快照生成 placement hint。
8. WorkflowCtl 执行原部署 Job。

### 5.3 容量补偿失败流程

容量补偿失败或超时时，Workflow 直接失败，原部署 Job 不执行。错误原因需要明确记录为容量补偿失败，包括 provider、action、nodeProfile、资源缺口和底层错误信息。

## 6. 资源评估模型

资源评估以 Kubernetes Pod 调度语义为准。

### 6.1 输入对象

第一版覆盖所有 Pod 型工作负载：

- Deployment
- StatefulSet
- Job
- CronJob
- 后续可扩展 DaemonSet、ReplicaSet 等

非 Pod 型资源不参与 CPU、Memory、GPU 计算：

- Service
- Ingress
- ConfigMap
- Secret
- PVC
- RBAC 资源
- CloudJob 本身

### 6.2 单 Pod 资源计算

普通 containers 的 requests 按资源类型求和：

```text
pod.cpu = sum(container.requests.cpu)
pod.memory = sum(container.requests.memory)
pod.gpu = sum(container.requests["nvidia.com/gpu"])
```

init containers 按 Kubernetes 调度语义取同类资源最大值：

```text
pod.resource = max(sum(app containers), max(init containers))
```

sidecar 按普通 container 参与求和。

### 6.3 副本数放大

Deployment、StatefulSet 按 replicas 放大：

```text
workload.resource = pod.resource * replicas
```

Job、CronJob 第一版按模板单次并发 Pod 数计算；如果后续支持 parallelism，应按 `parallelism` 参与计算。

### 6.4 未声明 requests 的处理

第一版建议按 0 处理，以保持与当前 traits 行为兼容。后续可以通过 scheduler policy 或全局配置增加默认 requests，例如：

```json
{
  "defaultRequests": {
    "cpu": "100m",
    "memory": "128Mi"
  }
}
```

如果 GPU 未声明，则不参与 GPU 容量计算。

## 7. 集群容量计算

Scheduler 需要读取：

- Node allocatable
- Node labels
- Node taints
- Node Ready 状态
- 当前 Pods 的 resource requests
- Pod 的 nodeName、nodeSelector、affinity、tolerations

单节点可用量：

```text
nodeAvailable = node.allocatable - sum(existingPod.requests on node)
```

集群可用量：

```text
clusterAvailable = sum(nodeAvailable for schedulable Ready nodes)
```

如果工作负载已有硬约束，例如 `targetWorkEnv`、nodeSelector、required nodeAffinity、tolerations，则只能在匹配节点集合内计算容量。

## 8. 容量补偿设计

### 8.1 CapacityPlan

当资源不足时，Scheduler 生成容量计划：

```json
{
  "planId": "<taskID>",
  "reason": "insufficient cpu/memory/gpu",
  "required": {
    "cpu": "8",
    "memory": "32Gi",
    "gpu": "1"
  },
  "available": {
    "cpu": "2",
    "memory": "8Gi",
    "gpu": "0"
  },
  "deficit": {
    "cpu": "6",
    "memory": "24Gi",
    "gpu": "1"
  },
  "nodeProfile": "default",
  "minReadyNodes": 1
}
```

### 8.2 CloudJob Action

容量补偿复用现有 cloudjob provider/action 体系。建议新增云动作：

```text
aliyun.ecs.ensureNodeCapacity
```

或定义更通用的动作名：

```text
cloud.node.ensureCapacity
```

推荐参数：

```json
{
  "provider": "aliyun",
  "action": "aliyun.ecs.ensureNodeCapacity",
  "params": {
    "planId": "<taskID>",
    "nodeProfile": "default",
    "cpu": "8",
    "memory": "32Gi",
    "gpu": "1",
    "minReadyNodes": 1,
    "waitNodeReadyTimeout": "15m"
  }
}
```

### 8.3 NodeProfile

云资源细节不建议散落在 Workflow 中，应放入系统配置或云 provider setting 中，由 Workflow 只引用 `nodeProfile`。

NodeProfile 应包含：

- 云厂商和地域配置
- 实例规格或规格选择策略
- 镜像 ID
- VPC、VSwitch、安全组
- 登录或 bootstrap 配置
- Kubernetes join 方式
- 节点标签
- 最大扩容节点数
- 成本或配额限制

### 8.4 完成条件

容量补偿成功条件必须同时满足：

1. 云侧 ECS 创建或节点池扩容成功。
2. 节点完成 bootstrap 并加入 Kubernetes。
3. Kubernetes Node 处于 Ready。
4. Node labels/taints 满足本次工作负载约束。
5. 新增或可用节点容量足以覆盖缺口。

只创建 ECS 不算成功。

## 9. Placement Hint 注入

容量满足后，Scheduler 可以为 PodTemplate 注入软调度提示。

新增节点建议打标签：

```text
eruun.io/scheduler-plan-id=<taskID>
eruun.io/scheduler-node-profile=<profile>
```

工作负载默认注入 soft nodeAffinity：

```yaml
affinity:
  nodeAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        preference:
          matchExpressions:
            - key: eruun.io/scheduler-plan-id
              operator: In
              values:
                - <taskID>
```

约束规则：

- 不覆盖用户已有 `targetWorkEnv`。
- 不覆盖用户已有 nodeSelector。
- 不覆盖用户已有 required nodeAffinity。
- 自动注入默认使用 soft affinity。
- 只有用户显式配置硬约束策略时，才允许注入 required affinity 或 nodeSelector。

## 10. Workflow 状态与可观测性

建议在任务状态或任务阶段中体现调度过程：

- `scheduling`：正在评估资源。
- `provisioning_capacity`：正在创建云资源。
- `waiting_node_ready`：云资源已创建，等待 Node Ready。
- `queued`：容量满足，等待 Worker 执行。
- `running`：原 Workflow 正在执行。
- `failed`：容量补偿失败或超时。

调度摘要应记录：

- 调度策略是否启用。
- 资源需求、可用资源和缺口。
- 使用的 provider/action/nodeProfile。
- capacity-provision Job 的执行结果。
- 新增节点名称或节点标签。
- placement hint 是否注入。

## 11. 幂等与恢复

容量补偿必须以 `taskID` 或 `planId` 作为幂等键。

要求：

- 同一 `taskID` 重试时复用同一个 capacity plan。
- Cloud action 重复执行时，不重复创建不可控 ECS。
- 如果 ECS 已创建但 Node 未 Ready，继续等待，而不是盲目再次创建。
- 进程重启后通过 cloudjob checkpoint 恢复执行状态。
- 如果 provider runtime 快照缺失且无法安全恢复，应 fail-fast 并要求重新执行任务。

## 12. 策略配置

建议在 Workflow 中增加可选 scheduler policy：

```json
{
  "schedulerPolicy": {
    "enabled": true,
    "scope": "allWorkloads",
    "admission": {
      "onInsufficient": "provisionCapacity"
    },
    "capacityProvisioning": {
      "provider": "aliyun",
      "action": "aliyun.ecs.ensureNodeCapacity",
      "nodeProfile": "default",
      "waitNodeReadyTimeout": "15m"
    },
    "placement": {
      "type": "softAffinity",
      "optimizeFor": "binpack"
    }
  }
}
```

默认建议：

- 第一版默认覆盖所有 Pod 型工作负载。
- 资源不足默认触发容量补偿。
- placement 默认使用 soft affinity。
- 扩容失败默认使 Workflow 失败。
- 如果 scheduler policy 启用但云配置缺失，应直接失败，不静默跳过。

## 13. 与现有模块的关系

### 13.1 WorkflowQueue

`WorkflowQueue` 继续作为任务持久化队列。Scheduler 不需要替换队列模型，但可以在任务内部记录调度摘要或阶段状态。

### 13.2 Dispatcher

Dispatcher 仍负责将 waiting task 发布到 Redis/Kafka。容量补偿逻辑不建议放在 Dispatcher 中，否则会把云资源创建和队列派发耦合过深。

### 13.3 WorkflowCtl

WorkflowCtl 是最合适的第一版接入点，因为它已经能生成完整 Job 计划，也能控制 Step / Priority 执行顺序。

### 13.4 CloudJob

CloudJob 是容量补偿动作的承载层。ECS 创建、节点池扩容、等待节点 Ready 都应通过 CloudJob action 的可恢复执行模型完成。

## 14. 风险与约束

- 自动创建 ECS 会带来成本风险，必须设置 nodeProfile 白名单、最大节点数和配额限制。
- 节点加入 Kubernetes 涉及凭据和 bootstrap 安全，需要独立审计。
- 用户已有硬调度约束时，新节点必须满足这些约束，否则扩容无意义。
- 如果仅变更 PodTemplate 调度字段，Deployment/StatefulSet 的更新判断必须覆盖 nodeSelector、affinity、tolerations。
- 资源评估只基于 requests，无法准确代表实时 CPU/Memory 使用率。
- Kubernetes Scheduler 仍可能因为亲和性、污点、PVC zone 等约束导致 Pending，因此 Node Ready 后仍需保留原部署失败路径。

## 15. 测试与验收

必须覆盖：

- Deployment、StatefulSet、Job、CronJob 的资源估算。
- replicas、sidecar、init container、GPU 的资源计算。
- 用户 nodeSelector / targetWorkEnv 约束下的节点集合过滤。
- 资源充足时不触发 capacity-provision。
- 资源不足时先执行 capacity-provision，原部署 Job 不提前执行。
- ECS 创建成功但 Node 未 Ready 时保持等待。
- Node Ready 后继续执行原 Workflow。
- 扩容失败、超时、provider 配置缺失时 Workflow 明确失败。
- placement hint 不覆盖用户已有硬约束。
- 重试或进程重启后不会重复创建 ECS。

## 16. 分阶段实施建议

### Phase 1：资源评估与容量补偿闭环

- 从 Job 计划提取 PodTemplate。
- 计算 Workflow 总资源需求。
- 查询 Node / Pod 快照并判断资源缺口。
- 资源不足时插入 capacity-provision 阶段。
- 通过 CloudJob 执行 ECS 创建和 Node Ready 等待。

### Phase 2：Placement Hint

- 为新增节点打确定性标签。
- 对工作负载注入 soft nodeAffinity。
- 修正 Deployment/StatefulSet 更新判断，确保调度字段变化会触发更新。

### Phase 3：策略与成本控制

- 增加 nodeProfile 管理。
- 增加最大节点数、最大预算、最大等待时间。
- 增加 scheduler 决策指标和事件日志。

### Phase 4：多云扩展

- 抽象通用 `ensureNodeCapacity` action。
- 支持更多 cloud provider。
- 支持不同节点池和异构资源类型。
