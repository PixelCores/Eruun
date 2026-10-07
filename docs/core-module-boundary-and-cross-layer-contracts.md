# 核心模块边界图与跨层字段契约（运行主链）

> 状态：Implemented Reference。本文作为跨 API/Domain/DB/Cache/K8s 字段语义的维护基线。

## 账号与空间归属

应用存在未停止任务时保留归属并拒绝删除。应用 `workspaceID` 为必填且不可跨空间修改；namespace 从空间派生。组件和应用工作流任务通过所属应用追溯空间；不属于某个应用的 resource import scan/manage、独立 `command` 与 `job` + `traits.eval` 任务在 `WorkflowQueue.WorkspaceID` 和 `JobInfo.WorkspaceID` 显式保存空间归属。HTTP 通过 X-Eruun-Workspace-ID 和成员关系授权，后台从持久化应用或任务自身的 workspace identity 确定空间。注册/保存不创建 Kubernetes 资源，首次部署先完成安全基线。完整契约见 [账号与空间](account-auth-workspaces.md) 和 [空间 Job](workspace-jobs-api.md)。

## 背景

在 Eruun 中，`app_id`、组件状态、Secret 引用等字段会跨 `API/Domain/DB/Cache/K8s` 多层流转。
如果缺少统一契约，容易出现“接口返回值正确但底层状态不一致”或“回写时编码语义错误”的问题。

本文定义运行主链 6 个核心模块的职责边界，并给出高风险核心字段的跨层契约表（表示、编码、脱敏、优先级）。

## 运行主链模块边界图

```mermaid
flowchart LR
    A[API契约层\ninterfaces/api + dto + assembler]
    B[应用用例与规则\ndomain/service + domain/repository]
    C[工作流/队列层\nworkflow service + event worker]
    D[缓存层\ninfrastructure/cache + Redis/Mem]
    E[K8s资源执行\njob ctl]
    F[Secret/Config转换层\nkube_convert + job_secret]
    G[(DB: eruun_applications/eruun_app_components/...)]
    H[(K8s API Server)]
    I[运行态观察\ninformer]

    A --> B
    B --> G
    B --> D
    B --> C
    C --> E
    E --> H
    E --> G
    H --> I
    I -->|ComponentStatusUpdate| B
    F --> B
    F --> E
```

## 模块职责与写入边界

| 模块 | 主要职责 | 可写边界 | 只读依赖 | 禁止越界 |
| --- | --- | --- | --- | --- |
| API 契约层 | 路由、参数校验、DTO 组装 | 不直接写 DB/K8s（仅调用 Domain） | Domain 返回对象 | 不在 handler 拼接跨层优先级逻辑 |
| 应用用例与规则 | 应用查询、组件聚合、契约归一、运行态更新裁决 | 应用/组件等 `eruun_*` 表、缓存失效/回填 | K8s 运行态观察 | 不直接拼接 K8s 编码细节到 API |
| 工作流/队列层 | 任务入队、调度、执行状态推进 | `eruun_workflow_queue`、`eruun_job` | 应用/组件元数据 | 不绕过 Domain 直接改 API 响应契约 |
| 缓存层 | `app:list:v3`、`app:template:list:v4`、`app:components:v6:<appId>` 读写 | 仅缓存键空间 | Domain 序列化对象 | 不成为事实源（Source of Truth） |
| K8s 资源执行与观察 | Job 控制器创建/更新资源；Informer 产生观察并委托 `SyncComponentStatus` | Job 执行路径写 K8s 和执行记录；Informer 回写由领域用例裁决 | 组件标签映射、任务上下文 | 观察路径不直接改配置主数据（名称、版本等）或覆盖生命周期状态 |
| Secret/Config 转换层 | K8s YAML <-> 组件模型转换、Secret 输入标准化 | 组件 `properties.conf/secret` 与 K8s ConfigMap/Secret 载荷 | URL 拉取与安全策略 | 不引入“自动猜测编码”造成语义漂移 |

## 状态修改权与事务边界

以下按数据责任区分写入路径，不表示已经拆分出独立数据库或限界上下文。定义、执行记录和执行载荷的术语见 [当前架构](架构文档.md)。

| 状态/数据 | 修改责任 | 必须保留的边界 |
| --- | --- | --- |
| 应用/组件期望配置、版本与 Workflow 定义 | `domain/service/application`、`domain/service/workflow` 的相应用例；资源导入按纳管契约建立配置 | 授权与空间归属校验；执行提交、版本变更通过应用调度事务串行化空闲检查和相关写入 |
| `WorkflowQueue` 执行状态、审批与 ownership | API 用例执行取消/审批；Scheduler claim/recover；Worker 的 `WorkflowCtl` 推进步骤 | 状态条件与 generation/token/worker fencing；队列消息不能直接成为权威状态 |
| `JobInfo` 执行状态与检查点 | Job 控制器、延迟/结果处理及 Runner 协议的相应执行路径 | execution key、attempt、父任务 ownership 或相应结果租约；恢复复用已提交身份 |
| 组件生命周期状态与运行态投影 | 应用/Job 生命周期路径设定执行状态；`SyncComponentStatus` 接受观察并裁决运行态写入 | Informer 传递 `model.ComponentStatusUpdate`；保护 `Not Deploy`、`Stopped`、`Cleaning` 等状态，按旧快照条件写入并使缓存失效 |
| 工作空间、成员关系与归属 | account 用例维护成员、角色及空间生命周期；运行时从持久化归属恢复 scope | 请求提供的空间标识不能替代成员校验；后台不能凭消息中的 namespace 获得任意空间权限 |
| 原始评测结果与交付状态 | Jobs 接受 Runner 结果；`jobs/artifacts` 保存源制品及各目标 `JobDelivery` | 接受结果时验证当前执行；源结果与待交付记录一起提交，每个目标独立租约、状态与重试 |

实际事务和外部副作用分界：

1. **应用调度事务。** `WithApplicationSchedulingTransaction` 使用 read-committed 事务锁定既有 Application 行，串行化空闲检查与执行提交，并承载该用例的配置、计划或组件写入。Redis 应用锁协调请求，数据库行锁仍防止锁失效后的重复提交；这不是把整个应用的运行过程放在一个长事务中。
2. **执行推进。** Dispatch claim 在 DB 条件写入后投递消息；投递失败归还 claim，进程中断由 lease 恢复。`WithWorkflowTaskOwnership` 在短事务中验证父任务身份并保护子 Job 写入。Kubernetes 创建/更新和 SQL 不构成原子事务，需要执行身份、检查点和恢复逻辑收敛。
3. **结果接受与目标交付。** `PutResultGuarded` 在事务内校验归属、保存源制品和目标交付记录；每个 `JobDelivery` 先在短事务内领取租约，再执行外部传输，最后以 lease token 条件写回。结果被接受与每个目标交付成功是不同事实。
4. **事务内 Kubernetes 副作用的现存限制。** `DeleteWorkspace` 在 `teamMutation` 的数据库事务回调内调用 namespace 删除，再删数据库记录；`commitDirectVersionUpdate` 在应用调度事务内可能调用 `cleanupVersionUpdateRemovedComponent` 清理移除组件。SQL 回滚不能撤销已发生的 Kubernetes 删除，版本更新路径也明确报告“数据库提交无法确认，Kubernetes 清理可能已生效”。这些路径仍需独立设计持久化意图、重试和部分完成语义，不能宣称 DB/K8s 原子一致；当前改动未调整其执行语义。

纯分类规则与持久化裁决也应区分：`model.IsWorkflowActiveStatus` 只分类状态；取消空状态的 API 规则、已取消任务的活跃 Job 查询、审批条件与并发 CAS 仍由各自用例执行。单个分类函数不代表全局状态机或聚合事务已经统一。

## 跨层字段契约总表（高风险核心字段）

说明：
- `Domain/DB` 列中的表名使用 `eruun_` 前缀。
- “优先级”表示读取同一业务语义时的事实源顺序，不代表调用链顺序。

| 字段 | API 表示 | Domain/DB 表示 | Cache 表示 | K8s 表示 | 编码/脱敏规则 | 事实源与优先级 |
| --- | --- | --- | --- | --- | --- | --- |
| `appId` | 路径参数 `:appID`；响应 `appId` | `Applications.ID`，组件侧 `ApplicationComponent.AppID`（`eruun_applications.id` / `eruun_app_components.app_id`） | `app:components:v6:<appId>` | 标签 `eruun.io/app-id` | 原样字符串；不脱敏 | DB 主事实源；K8s 标签用于关联与状态同步 |
| `componentId` | 组件响应 `id` | `ApplicationComponent.ID`（`eruun_app_components.id`） | 缓存内组件对象 `id` | 标签 `eruun.io/component-id` | 数字转标签字符串 | DB 主事实源；K8s 标签仅回写校验 |
| 组件版本变更规格 | 版本请求的 `components[]` | `spec.ComponentUpdateSpec` 定义共享字段；规则直接消费该规格，再写入相应组件记录 | 无独立表示 | 由变更计划生成或更新资源 | 保持现有 JSON 字段及校验；不增加平行 DTO 字段定义 | 规格归 Domain，传输封装归 API；规格本身不是持久化实体 |
| `componentName` | 路径参数 `:componentName`；响应 `name` | `ApplicationComponent.Name`（`eruun_app_components.name`） | 缓存内组件对象 `name` | 标签 `eruun.io/component-name` | RFC1123 名称在生成资源名时处理 | DB 主事实源；K8s 标签用于 informer 定位 |
| `namespace` | 应用/组件响应 `namespace` | `Applications.Namespace`、`ApplicationComponent.Namespace` | 缓存内组件对象 `namespace` | 对象 metadata.namespace | 原样字符串 | DB 主事实源；K8s 为运行落地位置 |
| `workflowId` | 响应 `workflowId` | `Workflow.ID`、`WorkflowQueue.WorkflowID` | 列表缓存中会携带默认 workflow ID | 无直接标签 | 原样字符串 | DB 主事实源 |
| `taskId` | 执行/取消/查询链路返回 `taskId` | `WorkflowQueue.TaskID`，`JobInfo.TaskID` | 无常驻缓存键 | Job 注解 `eruun.job/taskId`（间接） | 原样字符串 | `eruun_workflow_queue` 主事实源，`eruun_job` 为执行明细 |
| resource import `workspaceId` | 提交接口从认证 workspace scope 获取，不接受请求体覆盖 | `WorkflowQueue.WorkspaceID`、`JobInfo.WorkspaceID`（`workspace_id`） | 无 | scan/manage 执行前解析为唯一 workspace namespace | 不对用户输入开放；非敏感 | 任务表是 app-less import Job 的空间归属事实源；JobInfo 必须与对应 task 一致 |
| 独立 Job `type/spec/traits` 与组件 `traits.eval` | `POST /jobs` 请求；可省略 workspaceId，显式填写时必须匹配认证空间 | `WorkflowQueue.Type=job`，`JobSpec` 保存已校验规格；公共评测 type 为 job；`JobInfo.Type` 为 command/eval（内部执行器标识） | 无 | 固定空间 namespace，TaskID/执行代映射 Job 与 Pod 注解 | 每个评测 `JobInfo.EvaluationInfo` 的能力 Token 与执行 checkpoint 不对用户序列化 | 复用队列与一次性 Job；不创建占位应用或平行调度器 |
| 评测原始结果与保存目标 | 结果查询/下载与单目标重试 API | `JobArtifact` 描述任务包/源/DB副本，`ArtifactChunk` 保存完整字节，`JobDelivery` 保存每个目标独立状态与租约 | 无 | Runner 在退出前上传，按 Pod/Job UID 与已提交执行身份验证 | 凭据限内部传输，MinIO连接限管理员配置 | 原始源默认90天，清理不影响保存副本；保存重试不再次执行Agent |
| `component.status` | 组件响应 `status` | `ApplicationComponent.Status`（`eruun_app_components.status`） | 缓存会存储纠正后的状态快照 | Informer 依据 Pod 快照推导 Running/Pending/Failed/Unknown | 非敏感，不脱敏 | 读路径以 DB 为准；Informer 仅回写运行态 |
| `component.readyReplicas` | 组件响应 `readyReplicas` | `ApplicationComponent.ReadyReplicas` | 缓存随组件对象缓存 | 由 Pod Ready 数推导 | 整型，不脱敏 | DB 主事实源（由 informer 回写） |
| `component.lastAbnormal` | 组件响应 `lastAbnormal` | `ApplicationComponent.LastAbnormal` | 缓存随组件对象缓存 | 从 Pod 异常摘要提取 | 可包含敏感上下文，日志需谨慎 | DB 主事实源（由 informer 回写） |
| `schedulingClass` | Workflow step/subStep 请求和详情，`background/normal/high` | Workflow JSON → JobTask → JobInfo `scheduling_class`；省略继承父步骤/默认 normal | 无 | 不映射为 Pod priorityClass | 未知类拒绝 | DB 的已提交执行记录在恢复时保留原类；资源依赖顺序不变 |
| Job 调度状态 | task stages 的 `info` 展示状态、类、排队时间和原因 | JobInfo `scheduling_state/priority/queued_at/generation/owner_status/expires_at/reason` | 无 | 无 | ownership/deadline 仅内部使用 | JobInfo 队列 + SystemSetting `workflow_scheduler` 事务锁；独立于业务 status |
| `properties.jobRetryPolicy` | 同步立即执行 job 组件的 `onOOM`、次数、退避及资源增长上限 | Component Properties → Job annotation；JobInfo `internal_info` 保存尝试和资源快照 | 无 | `eruun.io/job-retry-policy` / `eruun.io/job-attempt`；UID 保护删除重建 | checkpoint 不作为公共请求字段 | DB checkpoint 保留跨 generation 预算和 deadline；详见 [失败策略](workflow-failure-policy.md) |
| `workflow_queue.status` | 任务状态相关 API 输出 | `WorkflowQueue.Status`（`eruun_workflow_queue.status`） | 无 | 无 | 非敏感 | `eruun_workflow_queue` 主事实源 |
| `workflow_queue.cancel_source` | 取消任务响应可见 | `WorkflowQueue.CancelSource`（`eruun_workflow_queue.cancel_source`） | 无 | 无 | 非敏感 | `eruun_workflow_queue` 主事实源 |
| `templateEnabled` | `ApplicationBase.templateEnabled` | `Applications.TemplateEnabled`（`eruun_applications.tmp_enable`） | `app:list:v3` / `app:template:list:v4` | 无 | 布尔值 | DB 主事实源，缓存只做加速 |
| `properties.secret[*]` | 组件 `properties.secret`（`secret` 组件） | `ApplicationComponent.Properties` JSON 内 `secret` map | 缓存中的组件 `properties.secret` | `Secret.StringData/Data` | 仅文本语义；不自动 base64 解码 | Domain/DB 为输入事实源；K8s 由 API server 完成字节化 |
| `credentials.value` | `components[].credentials[].value` | 由 assembler 从同应用同命名空间 `secret` 组件推导，不单独落库 | 随组件列表缓存 | 不直接来自实时 K8s Secret 读取 | 明文返回，必须按敏感信息处理 | Domain 组装结果；来源仍是 DB 中 `properties.secret` |
| `credentials.resolved` | `components[].credentials[].resolved` | 运行时推导布尔值，不单独落库 | 随组件列表缓存 | 无 | `value` 为空或缺失则 `false` | Domain 组装结果 |
| `traits.envs[].valueFrom.secret` | 组件 traits 内 secret key 引用 | `ApplicationComponent.Traits` JSON | 随组件列表缓存 | 最终映射为容器 `EnvVarSource.SecretKeyRef` | 仅引用，不携带值 | Traits 配置为事实源，值解析依赖 `secret` 组件 |
| `traits.envFrom[].sourceName(type=secret)` | 组件 traits 内整包 Secret 引用 | `ApplicationComponent.Traits` JSON | 随组件列表缓存 | 最终映射为 `EnvFrom.SecretRef` | 仅引用，不携带值 | Traits 配置为事实源 |
| `traits.storage[].sourceName(type=secret)` | 组件 traits 存储引用 | `ApplicationComponent.Traits` JSON | 随组件列表缓存 | 最终映射为 `Volume.Secret.SecretName` | 仅引用，不携带值 | Traits 配置为事实源 |
| `k8s secret payload` | API 不直接暴露该底层形态 | `SecretInput.Data`（字符串 map） | 无 | 创建时主要通过 `Secret.StringData`，落地后 K8s 存为 `Data` 字节 | K8s API 自动进行 base64 传输层处理 | Domain 输入语义优先；K8s 为承载格式 |
| `app:list:v3` 缓存键 | 列表接口响应 | 来源 `Applications` + 默认 workflow 推导 | 键 `app:list:v3` | 无 | JSON 序列化缓存 | DB 优先，缓存可失效重建 |
| `app:template:list:v4` 缓存键 | 模板列表接口响应 | 来源 `Applications.TemplateEnabled` + workflow 推导 + `resources` 摘要 | 键 `app:template:list:v4` | 无 | JSON 序列化缓存 | DB 优先，缓存可失效重建 |
| `app:components:v6:<appId>` 缓存键 | 组件列表接口响应 | 来源 `ApplicationComponent` + 读路径纠正状态 | 键 `app:components:v6:<appId>` | 状态变更时由 informer 触发失效 | JSON 序列化缓存 | DB 优先，缓存仅加速 |
| `adoptionSnapshot.resources[].pendingRecreation.token` | 不作为公共 API 字段返回 | `Applications.AdoptionSnapshot`（`eruun_applications.adoption_snapshot` JSON）内的写前重建声明 | 无 | 同值写入待创建对象注解 `eruun.io/adopted-recreation-token` | opaque UUID，不承载 Secret；仅用于匹配一次重建声明与替代对象 | DB 中的 pending claim 是声明事实源；K8s 注解只有与当前 pending token 一致时才可用于恢复/完成重建 |

## Adopted 资源重建的一致性契约

`ResourceSnapshot.PendingRecreation` 不是第二套 Kubernetes ownership：资源所有权仍由 adoption snapshot 中的 `source.uid` 约束。它是缺失的 adopted 资源在重建期间使用的 write-ahead claim，用于跨重试、进程重启和 `Create` 结果不确定场景，将一份已持久化意图与一个替代对象绑定起来。

| 阶段 | DB / Domain 状态 | Kubernetes 状态 | 一致性要求 |
| --- | --- | --- | --- |
| Prepare | 先按 app ID + resource identity 获取可续期的分布式 recreation lease，再在锁内重新加载 canonical application snapshot；仅允许 `ownership=exclusive`、`disposition=managed`、具有旧 `source.uid` 和可重建 manifest 的资源。若没有 claim，先通过 application `update_time` CAS 写入非空 token；已有 claim 时复用其 token | 尚未创建替代对象；候选对象复制原注解并写入 `eruun.io/adopted-recreation-token=<token>` | lease 不可用时 fail closed；claim 持久化成功后才能调用 Kubernetes `Create`，且当前 recreate 调用持有 lease 直到 Create、`AlreadyExists` 恢复与 finalize 结束。等待 lease 的旧执行者取得锁后必须先重读 canonical UID；CAS、UID 或前置校验失败时不得写 K8s |
| Create / recover | pending claim 保留，旧 `source.uid` 仍是 ownership baseline | `Create` 成功、返回 `AlreadyExists`，或下一次 reconcile 发现同名对象时，均可进入恢复 | `AlreadyExists` 恢复复用当前 recreate 已持有的 lease；后续 reconcile 的恢复先获取同一 resource lease，且不会重新执行 `Create`。只有对象的 namespace/name 符合 snapshot、UID 非空且不同于旧 UID、未处于 terminating，并且 annotation token 等于当前 pending token，才能将其视为本次重建结果 |
| Finalize | 在同一 resource lease 内重新加载 canonical snapshot 并再次校验 claim；把 `source.uid`、`resourceVersion`、`specDigest` 更新为替代对象，随后清除 `pendingRecreation`。Workload 同一事务更新 `ApplicationComponent.SourceWorkloadUID`；Secret 同一事务更新相关加密数据；普通依赖更新 application snapshot | 替代对象继续存在 | 事务/CAS 成功只确认 ownership 迁移；任务仍须按当前 desired state 完成本类型的正常调和后才能成功。若另一执行者已经提交相同对象 UID 且 claim 已清除，恢复路径将其视为已完成，而不是重复写入；任何推进 UID 或清除 claim 的路径都不得绕过该 lease |
| Persistence failure | 明确确认未提交时保留旧 `source.uid` 与 pending claim；若提交结果不明确，则回读 canonical snapshot 与 workload 绑定，回读也失败时 DB 状态保持 unknown（可能仍 pending，也可能已经完成） | 已创建的替代对象保留，不执行补偿删除 | 采用 no-rollback：删除可能误删已被另一执行者确认的对象。后续 reconcile 重读 canonical 状态：仍 pending 时按同一 token 恢复，已写入替代 UID 时按已完成处理；无法确认时返回可观测错误，仍不删除对象 |

Finalize 清除的是 DB snapshot 中的 `pendingRecreation`。替代对象上的 recreation token 注解不会在 finalize 中主动删除，但 claim 清除后它不再构成有效的重建授权；生成 snapshot manifest 或计算 `specDigest` 时会剥离该注解，因此它不会污染期望配置或制造永久 drift。

版本兼容边界如下：

- Snapshot v1 与 v2 都可读取和校验；v1 不允许携带 `pendingRecreation`。
- 新建 adoption snapshot 使用 v2。历史 v1 snapshot 在没有 pending claim 时与等价 v2 snapshot 按同一导入契约比较，不要求为了读取而立即回写迁移。
- 首次为历史 snapshot 持久化 recreation claim 时，写入版本升级为 v2；空 token、缺少旧 `source.uid`、非 exclusive/managed 资源或缺少 manifest 都会校验失败。
- v2 的新增语义仅是 write-ahead recreation claim。Token 不是 Secret，也不是长期 ownership 事实；它只授权当前 pending transition。Finalize 校验当前 token、对象 identity、非 terminating 状态以及非空且不同于旧值的新 UID，成功后由写回的 Kubernetes UID 继续承担 ownership 约束；谁可创建或修改带 token 的对象仍由 Kubernetes RBAC 与 exclusive management 边界控制。

## 关键优先级规则（必须保持）

1. 组件查询读取优先级  
`Cache(app:components:v6:<appId>) -> DB(eruun_app_components)`；未命中或缓存损坏时回源 DB。  
K8s 不是组件查询实时事实源，而是通过 informer 异步回写 DB。

2. 组件状态保护规则  
当 DB 状态为 `Not Deploy`、`Stopped` 或 `Cleaning` 时，Informer 观察不得随意覆盖；由 `SyncComponentStatus` 判定，`Cleaning` 在观察到副本清零后才转 `Not Deploy`。观察载荷不携带状态修改权，写入仍需通过旧快照条件校验。

3. Secret 值语义规则  
`properties.secret` 与 `credentials.value` 按文本字面量处理；即使看起来像 base64，也不在读路径自动解码。  
导入/转换 K8s Secret 时仅接受可表示为 UTF-8 文本的数据。

4. 标签关联规则  
`eruun.io/app-id`、`eruun.io/component-id` 与 `eruun.io/component-name` 必须完整参与 K8s 资源关联与状态同步定位，缺一则不能视为可回写目标。

5. 缓存一致性规则  
组件状态更新、应用增删改等会触发对应缓存失效；缓存失败不影响主流程正确性，DB 保持事实源。
缓存读写与读取过程中损坏条目的清理透传调用者 `context.Context`；Redis 在父 context 上追加 5 秒操作上限（`List` 为 30 秒），客户端启用 context deadline。数据变更后的缓存失效保留 context 值，但使用独立的 5 秒超时，不随原请求取消或到期而跳过删除；应用更新、Informer 和 Job 状态同步遵循同一规则。等待连接或重试时可响应取消；已经进行的 socket I/O 仍受 go-redis 的读写 deadline 控制，手动取消不保证立即中断该 I/O。
锁与 Workflow 取消信号由 server 显式提供 Redis/Locker 依赖，不经缓存对象获取。使用内存缓存或禁用读取缓存不会禁用协调能力；协调依赖缺失仍拒绝相应操作。

6. Adopted 重建声明规则
必须先取得按 app/resource 隔离的可续期 lease，并在锁内重读 canonical UID、持久化 `pendingRecreation.token`，再创建带同值注解的替代对象；当前 recreate 调用持有 lease 到绑定完成，后续 reconcile 也必须取得同一 lease 才能推进 UID 或清除 claim。创建后的 DB 写入失败始终保留 live object：明确未提交时保留 claim 供重试恢复，提交状态无法确认时由后续 reconcile 在 lease 内重读 canonical UID/claim 收敛；两种情况都不做补偿删除。

## 变更前检查清单（面向新增/调整返回字段）

- 是否明确该字段在 `API/Domain/DB/Cache/K8s` 五层的表示与命名？
- 是否定义了编码语义（明文/UTF-8/base64 由谁处理）？
- 是否定义了脱敏策略（返回、日志、埋点）？
- 是否定义了事实源优先级与冲突处理？
- 是否补充了跨层回归用例（至少覆盖读路径 + 状态同步/回写路径）？

## 相关实现锚点

- 组件读路径与缓存：`pkg/apiserver/domain/service/application/application_query.go`、`pkg/apiserver/domain/service/application/application_cache.go`
- 组件 API 组装与 credential 解析：`pkg/apiserver/interfaces/api/assembler/v1/component.go`
- 状态同步：`pkg/apiserver/infrastructure/informer/waiter.go`、`pkg/apiserver/domain/service/application/component_status_sync.go`；`pkg/apiserver/server_status_sync.go` 仅提供 5 秒有界回调
- 应用调度事务和执行 ownership：`pkg/apiserver/domain/repository/application_scheduling.go`、`pkg/apiserver/domain/repository/workflow_lease.go`
- 直接版本更新中的资源清理：`pkg/apiserver/domain/service/application/application_update_version_phases.go` 中的 `commitDirectVersionUpdate`
- 空间删除：`pkg/apiserver/domain/service/account/workspaces.go` 中的 `DeleteWorkspace`、`teamMutation`
- 结果接受和交付：`pkg/apiserver/jobs/artifacts/store.go`、`pkg/apiserver/jobs/artifacts/delivery.go`
- Secret 落地与编码边界：`pkg/apiserver/event/workflow/job/job_secret.go`
- 纳管 snapshot 版本、校验与 digest 归一化：`pkg/apiserver/domain/service/resourceimport/contract/snapshot.go`
- Adopted 重建 claim、恢复与 finalize：`pkg/apiserver/event/workflow/job/job_adopted_source.go`
- Recreation token annotation 常量：`pkg/apiserver/config/consts.go`
- 模型与表映射：`pkg/apiserver/domain/model/*.go`

评测的模型、harness、任务包、并发与预算由共享 `EvaluationTraitSpec` 定义。独立 Job 和 Application 的顶层 job 组件共享构建器；框架版本由 Runner 固定并记入执行快照。结果制品和保存目标用 WorkspaceID、TaskID、ExecutionKey 区分；应用内多个评测不能仅以父 TaskID 寻址。
