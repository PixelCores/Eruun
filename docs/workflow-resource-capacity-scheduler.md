# Workflow 资源容量准入与补偿方向

> 状态：Draft / Proposal。本文描述 Workflow 执行前的资源容量准入与可选容量补偿边界；当前运行链路尚未实现该能力，本文不定义公共字段、CloudJob action、节点标签、状态值、默认策略或固定超时。

## 1. 当前事实

Eruun 当前由 Scheduler 派发 waiting Workflow，由 Worker 执行 Workflow/Job。Pod 的最终放置仍由 Kubernetes Scheduler 决定；当资源或调度约束无法满足时，工作负载会进入 Pending。

当前 `resources` Trait 只稳定表达 CPU、内存和 `nvidia.com/gpu`，`targetWorkEnv` 映射为 `nodeSelector`。Eruun 尚未在 Workflow 派发前聚合集群容量，也没有自动创建计算节点或扩容节点池的内置 CloudJob action。

## 2. 目标与非目标

目标：

- 在开始创建工作负载前识别明显无法满足的资源需求。
- 区分可立即执行、需要等待、需要人工处理和允许容量补偿的任务。
- 在策略明确允许时，通过版本化 Provider action 请求外部容量并等待 Kubernetes 可调度资源就绪。
- 保持决策、外部副作用、重试和最终结果可解释、可恢复、可审计。

非目标：

- 替代 Kubernetes Scheduler 或直接把 Pod 绑定到 Node。
- 依据实时使用率承诺调度结果。
- 在没有预算、配额、身份和审批边界时自动购买云资源。
- 把特定云实例、节点池 API、GPU 插件或调度算法写入 Eruun 公共契约。

## 3. 容量准入边界

容量准入应使用最终将提交给 Kubernetes 的 workload 计划作为输入，而不是只读取原始 Application JSON。最低需要考虑：

- app container、init container、sidecar 的 resource requests。
- Deployment/StatefulSet 副本和 Job 并行度。
- nodeSelector、taint/toleration、affinity、拓扑和设备资源等硬约束。
- Node Ready/可调度状态、allocatable 以及现有 Pod requests。
- PVC zone、设备插件和 operator 等会影响可调度性的集群能力。

第一版只能把结果解释为准入判断，不能保证 Kubernetes 最终一定调度成功。容量快照和实际绑定之间存在竞争，正常 Pending、失败和取消路径仍必须保留。

通用容量准入与 [Workflow 全局调度方向](workflow-global-scheduler-design.md) 串联，但职责不同：全局调度决定哪个 Workflow 获得执行机会，容量准入判断该 Workflow 的资源意图是否可满足。

## 4. 容量补偿

容量补偿是可选的外部副作用，不是资源不足时的默认行为。只有满足以下条件才可启用：

- workspace/project 已绑定允许使用的 Provider、区域、容量类型、配额和预算。
- 调用者或平台策略有权触发对应动作，高成本动作可要求人工审批。
- Provider action 提供幂等身份、异步状态、取消/补偿边界和审计结果。
- 新容量加入集群的身份、bootstrap、标签、taint 和凭据流程已经过安全评审。
- Eruun 能在 Worker 重启或消息重复后恢复等待，而不会重复创建不可控资源。

外部资源创建成功不等于容量可用。补偿只有在目标资源已进入 Kubernetes、满足声明的硬约束并通过重新准入后，才可以让原 Workflow 继续。

具体 Provider/action 设计遵循 [AI Provider 集成方向](ai-provider-integration-design.md)。当前 CloudJob 内置能力只覆盖阿里云 NAS 与 StorageClass 引导，不能被描述成已经支持计算节点扩容。

## 5. 调度提示

容量准备完成后，可以评估是否为 workload 增加软性 placement hint。任何 hint 都必须：

- 不覆盖用户已有的 nodeSelector、required affinity、toleration 或拓扑约束。
- 不把某次任务身份、Provider 名称或内部计划字段升级为公共标签契约。
- 在更新判断、回滚、状态展示和审计中可见。
- 缺失时只影响优化效果，不改变权限或隔离边界。

硬性 placement 必须由调用方显式声明并通过验证，不能由容量补偿过程悄悄注入。

## 6. 状态、所有权与恢复

- 准入结果、容量预留和 Workflow ownership 必须以数据库事务/CAS 为权威边界。
- 消息重复或 Leader 切换不能造成重复占用配额或绕过 generation/token fencing。
- 外部容量请求使用稳定幂等键；结果未知时先查询，不盲目再次创建。
- 容量等待需要可序列化 checkpoint，进程内 client 或 Token 不能成为恢复事实。
- 取消要停止后续执行并记录外部资源是否仍存在；只有显式补偿能力才能宣称自动回收。
- 日志与 trace 不包含 bootstrap 凭据、云密钥或加入集群的敏感材料。

## 7. 可观测性

最低观测面包括：

- 资源需求、候选节点集合、容量快照版本和准入结果。
- 不可满足的资源或硬约束及其解释。
- 配额/预算拒绝、审批等待和 Provider 调用状态。
- 容量请求、幂等恢复、新资源就绪、重新准入和最终收敛。
- 准入判断与 Kubernetes 实际 Pending 原因的偏差。

高基数 workspace、task、node 和外部资源身份应进入结构化日志或 trace，不直接作为无界指标 label。

## 8. 实施门禁

1. 先以只观察的 shadow 模式，从当前 workload 计划计算需求和候选节点，不改变执行结果。
2. 用代表性 workload 对比准入判断与 Kubernetes 实际调度结果，记录误判边界。
3. 增加不产生外部副作用的等待/拒绝策略，并验证 Leader 切换、消息重复、取消和回滚。
4. 选择一个明确 Provider action 完成幂等容量补偿、安全、预算、审批和恢复闭环。
5. 只有软性提示被真实负载证明有价值后，才增加 placement hint。

升级为 Current 前必须提供实现与测试、公共契约（如有）、Provider 版本矩阵、真实集群验收、成本与安全控制、故障恢复和运维指标。
