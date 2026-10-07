# 过度设计与代码膨胀审计及整改（2026-10-02）

> 状态：Historical / Audit。审查基线为 `main@0984611debb2cc299fdfba186f5eb73ae3993d3f`。原始代码证据固定到该提交；PR #123 分支已实施 O16–O21，处置与验证见文末，不代表已经合入 main。下文发现描述的是审查时状态，不把 Draft / Proposal 当成当前能力。

> 当前拓扑提示：下文的四角色、双 Lease 和对应测试属于所列历史基线。当前统一节点使用单 Leader，见[分布式运行时设计](enterprise-distributed-runtime-design.md)与[部署契约](helm-deployment.md)。历史证据不作为新拓扑验收结果。

## 结论与判断标准

本轮确认 **6 项 P3 维护债务或配置／查询表达问题**。主要问题不是项目使用了多少层，而是部分路径为同一需求维护了两套表示、丢失信息后再反推、重复解释同一持久化规则，或者保留了已脱离生产调用的业务条件。下文说明可达入口、额外成本和最小简化边界；没有依据这些发现声称发生过线上事故，也没有测得性能收益。

沿用历史报告的编号，从 O16 开始，避免与已处置的 O01–O15 混淆。六项均已在本 PR 分支整改；保留原始发现及最小修复边界，便于核对改动理由。

| 编号 | 问题 | 具体维护成本 | 建议范围与风险 |
| --- | --- | --- | --- |
| O16 | Service 使用 ApplyConfiguration 中间表示，执行却是普通 Create/Update | 两套对象表示、手写转换及多处 metadata 特判 | 统一内部 Service 表示；中等风险，保留资源分配与 adopted 语义 |
| O17 | Observe 导入丢失来源，再用内容签名和顺序反推 | JSON 签名预算、名称队列与转换排序需同步维护 | 保留内部来源映射；中等风险，需覆盖同名与共享依赖 |
| O18 | StatefulSet 清理引用规则由两处重复解释 | 同一持久化字段的规范化与环校验有两份实现 | 只共享无副作用规则；涉及数据删除保护，验证要求高 |
| O19 | 持久化记录兼任查询，字段是否生效由隐藏白名单决定 | 查询意图散落四处，取消查询中的 Status 实际被忽略 | 先显式表达这一条查询；范围小，保留清理保护 |
| O20 | 两个 tracing 开关表达同一个启用状态 | 四种组合、额外文档说明和测试矩阵 | 收敛公开配置并明确迁移，属于配置契约变更 |
| O21 | HTTP 重构后旧业务条件仍由测试保活 | 测试验证脱离当前生产路径的第二份规则 | 删除私有残留、迁移有价值断言；低风险 |

## 范围与证据强度

从 `docs/README.md` 路由到各模块，结合实际调用链、全仓符号检索和既有测试进行审查。这里的“全仓”指覆盖主要模块，**不是逐行穷尽每个文件或证明不存在其他问题**。

| 范围 | 本轮审查深度 | 结果及边界 |
| --- | --- | --- |
| `cmd/`、`config/`、`server*` | 直接阅读配置注册、启动装配、角色条件、tracing 与停止边界；抽查队列和 Leader 装配 | O20；已按角色装配的接口不重复列问题 |
| HTTP/gRPC、DTO/assembler、领域服务与模型 | 深读 create-and-exec、状态查询、namespace import 分流、版本清理重试和 Workflow 围栏；抽查账号 store、repository/spec 与转换 | O17、O18、O21；未逐一审查所有路由和领域方法 |
| Workflow/Job/Traits/naming | 深读 Service 构造／执行／导入、Instant/Scheduled、JobInfo、Trait 处理与 CloudJob 注册；抽查共享锁及清理 | O16；数据库恢复和资源身份保护保留 |
| Jobs/Harbor 与 infrastructure/utils | 直接阅读 cache、locker、消息队列、SQL driver、Informer 和 artifact delivery；抽查 Runner 授权、恢复及 Python 事件上报入口 | O19；未穷尽 Python agent 恢复协议和所有 Sandbox 状态组合 |
| 部署、构建与辅助脚本 | 抽查 Helm 角色模板、名称／凭据保护、安装器两种入口及 CI/Makefile | 未找到足够证据将安全安装、持久化身份保护或四角色部署列为过度设计 |

生成的 Protobuf Go 绑定、第三方依赖源码未逐行审查；历史与 Proposal 文档只用于定位背景。没有执行真实 MySQL、Redis、Kafka、Kubernetes、ACS 或云 SDK 验收，也没有做负载、故障注入或仓外 Go 消费者调查。

## O16｜普通 Service CRUD 维护了两套 Kubernetes 表示

**当前需求与调用链。** `properties.ports` / `traits.service` 经 [组件 builder](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job_builder_component.go#L649-L676) 创建 Service，放入 `JobTask.JobInfo`，再由 `DeployServiceJobCtl` 创建或更新集群资源。

**证据与额外成本。** [GenerateService / GenerateServiceFromTrait](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job/job_service.go#L375-L473) 先构造指针字段较多的 `ServiceApplyConfiguration`；[serviceFromApplyConfig](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job/job_service.go#L583-L634) 又手工映射为 `corev1.Service`。最终 [ApplyService](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job/job_service.go#L746-L800) 调用 `Get/Create/Update`，并未使用 Kubernetes server-side apply。新增一个支持字段时，维护者要同步检查两套表示和转换逻辑。

这不是只多了一次函数调用：该类型不满足普通资源的 `metav1.Object` 路径，导致 [JobInfo labels/annotations](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job/job_info.go#L628-L679)、[builder metadata](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job_builder_info.go#L59-L86) 和 [adopted manifest 解码](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job_builder_adopted_dependencies.go#L669-L680) 都需要专用分支。本轮未复现字段遗漏或错误更新，结论限于已经存在的维护表面。

**最小简化方向。** builder、JobInfo 和执行器内部统一使用 `*corev1.Service`，删除这层转换，让 metadata 和 manifest 处理复用已有资源路径。保持现在的 Create/Update 策略，不借此引入 SSA 或统一资源框架。导出的 `GenerateService*` / `ApplyService` 签名会变化，实施前需核查仓外 Go 源码接入。

**必须保留与验证。** 保留默认 Type/TCP/端口名/targetPort、显式服务名、selector 归一化、Headless/ExternalName、share、adopted UID／重建保护、冲突重试和无变化不写入；尤其保留已有 ClusterIP/ClusterIPs/IPFamilies/NodePort 等分配字段。现有 `TestGenerateService*`、`TestApplyService*`、`TestServiceNeedsUpdate*` 及 adopted Service 测试应继续覆盖真实执行链；metadata 与 adopted manifest 解码也需回归。fake client 通过不等于真实 API server 的默认化行为已验收。

## O17｜Observe 导入先丢失来源，再从组件内容反推

**当前需求与可达范围。** namespace import / try 的 [HTTP 入口](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/interfaces/api/application_lifecycle.go#L63-L84) 需要在资源转换和组件重命名后保留“哪个资源对应哪个组件”。本项针对 observe/try 的 `buildImportPlans` 路径；新 resource-import manage Job 固定走 adopted 的显式 membership，不能将这个结论推广到所有导入模式。

**证据与额外成本。** [转换器](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/conversion/kube_convert_pipeline.go#L35-L104) 只返回组件、警告和错误，没有来源关系。[convertImportPlanComponents](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/resourceimport/namespace_import.go#L2037-L2066) 在存在共享模板时分别转换本应用资源与加入模板后的集合；[名称去重之后](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/resourceimport/namespace_import.go#L2111-L2118)，再用 [signature budget 和名称队列](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/resourceimport/namespace_import.go#L2355-L2407) 重建映射。

为此，[辅助逻辑](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/resourceimport/namespace_import.go#L2459-L2534) 将 Properties/Traits 等组件内容序列化成 JSON 签名，并在第二处复制 ConfigMap → Secret → workload → Job → CronJob 的转换顺序。转换顺序、组件字段或同名处理变化时，维护者必须同时检查这套反推规则；结果实际用于 [Component ID 解析和资源标签写入](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/resourceimport/namespace_import_execution.go#L425-L437)。现有测试通过，不能据此声称当前已标错资源。

**最小简化方向。** 在现有内部转换流程直接保留源 resource key、共享来源与组件位置的对应关系，名称去重时同步更新映射。公共 DTO 不增加字段，不引入 registry 或通用映射框架。共享依赖合并可能仍需要分别处理两组输入，不能仅因转换执行两次就全部删除。

**必须保留与验证。** 同名不同 Kind、同名共享模板、内容相同的本地／共享组件、稳定重命名、Service/RBAC 依赖归属、StatefulSet volumeClaimTemplates 跳过规则，以及原有标签语义。`namespace_import_plan_test.go` 中的 `TestBuildImportPlans_*`、`TestBuildResourceComponentNameMapping_*`、`TestEnsureSharedComponentsOnApp_*` 是现有行为基线；后续应验证改变内部遍历顺序不会改变来源归属。

## O18｜清理任务的同一引用规则维护了两份实现

**当前需求。** [`VersionUpdateCleanupInfo.ResolvesTaskIDs`](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/model/workflow_queue.go#L48-L55) 明确记录后续清理任务解决了哪些前序任务。版本更新恢复入口据此构造剩余清理，Workflow 执行围栏据此决定是否允许新任务；因果关系抵抗相同时间戳和时间倒退，有实际必要性。

**证据与额外成本。** [版本恢复入口](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/application/application_update_version_cleanup_retry.go#L411-L469) 与 [Workflow 围栏](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/workflow/statefulset_cleanup_fence.go#L318-L376) 各有一份同形实现：去空白、拒绝空值／自引用／重复引用、排序、未知节点检查和 DFS 环检测。它们消费的是同一个持久化字段；今后修订合法引用规则，必须同步修改两处。当前两份规则一致，本轮没有发现实际行为分歧。

**最小简化方向。** 由现有领域所有者承载这组窄的、无副作用的引用校验规则，两入口复用。不要新增泛型图框架，不合并两种 pending 数据结构，也不合并“恢复清理”和“拒绝继续执行”的业务决策。覆盖范围、版本标记和资源 identity 的判断继续留在各自流程。

**必须保留与验证。** 在恢复入口和执行围栏两侧对称验证：时间倒退／相同时间、未知引用、环、自引用、重复引用、多 resolver、已成功但覆盖不全、不同资源 identity，以及 V2/V3 和 PVC 模板边界。现有 `statefulset_cleanup_fence_test.go` 与 `application_update_version_cleanup_retry_additional_test.go` 的显式因果、未知引用和环检测测试是基线。不能为了减少代码删除清理 fence、CAS 或实际删除前的身份校验。

## O19｜查询条件隐藏在记录模型的 Index 方法中

**当前需求与证据。** Controller [周期恢复取消清理](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/workflow.go#L176-L208)，需要查找 Cancelled 且具有精确清理标记的 Job。[查询调用](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job/cancel_recovery.go#L44-L83) 传入 `JobInfo{Status: ...}`，但 [`JobInfo.Index()`](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/model/job.go#L104-L115) 只选 workspace/task/workflow 字段。SQL driver [只使用 Index 与 FilterOptions](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/infrastructure/datastore/sql/driver.go#L294-L329)，不会自动把其余实体字段加入条件。

因此这条查询没有 `status = Cancelled` 条件，实际筛选为 `type IN (...)` 加 `scheduling_reason LIKE '%parent workflow cancelled%'`；LIKE 来自 [FuzzyQueryOption 的实现](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/infrastructure/datastore/sql/driver.go#L255-L270)。相同写法在 [`WorkflowQueue{Status: ...}`](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/model/workflow_queue.go#L98-L115) 上却生效。

**额外成本与影响边界。** 一次精确筛选的含义散落在实体参数、模型字段白名单、过滤选项和读后判断四处。调用者必须逐模型记住哪些字段有效；现有 [测试替身](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/event/workflow/job/cancel_recovery_test.go#L40-L42) 直接返回记录，不能发现查询条件遗漏。

每页最多 100 条，外层有超时和翻页，读后还做精确状态／标记检查及执行身份保护。正常路径在同次 CAS 中写入 Cancelled 与清理标记，因此一致数据未必带来多余结果。其他状态或只包含标记子串的记录可能占用候选页；本轮**没有误删、无界结果读取、永久饥饿或数据库性能实测证据**。

**最小简化方向。** 先在这一查询用现有 `InQueryOption` 的单值集合明确表达 status 与完整 scheduling_reason，移除无效的实体 Status 形参和模糊匹配。保留读后检查、UID/execution key/generation/attempt、CAS、分页和超时。不因此更换全仓 ORM、扩充所有模型的 Index 或新建查询框架。

**验证要求。** 使用真实 SQL 条件生成或隔离数据库验证正确取消记录被选中，其他状态、标记子串和错误类型被排除，并验证超过 100 条的翻页；现有替换执行身份／CronJob 保护测试必须继续通过。固定返回记录的替身不能独自证明查询正确。

## O20｜两个 tracing 开关只表达一个逻辑或

**当前需求与证据。** 运行时需要决定是否启用 tracing，并独立决定是否向 Jaeger 导出。配置同时暴露 [`EnableTracing` / `AutoTracing`](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/config/config.go#L345-L347)，而 [`resolveTracing`](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/cmd/server/app/server.go#L222-L226) 只执行 `EnableTracing || AutoTracing`，不根据消息后端、环境或 exporter 做自动判断。额外返回值仅用于输出一条启用来源日志。[静态部署](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/deploy/eruun-stack.yaml#L475-L476) 同时设置两者为 true。

**额外成本。** 一个布尔状态变成四种组合，帮助文字必须解释“把 enable-tracing 设为 false 仍可能开启”；用户关闭时要同时设置两个参数。[现有测试](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/cmd/server/app/server_test.go#L47-L89) 也维护四种组合及后端/exporter 矩阵。这是已公开且已测试的行为，不是 flag 解析错误，也不能把现有测试直接当冗余删掉。

此前 [PR #111](https://github.com/PixelCores/Eruun/pull/111) 已去掉无效的环境判断并明确逻辑或语义。本项讨论剩余公开参数面的简化，不将那次已完成的整改重新列为未修问题。

**最小简化方向与门槛。** 在明确的配置变更中收敛为单一 tracing 开关，保留 Jaeger endpoint 的独立职责，更新 manifest、默认配置、帮助和文档。先调查部署中的旧参数并明确迁移／拒绝方式；不要偷偷改变默认值或再新增兼容开关。当前 API middleware 与 tracer provider 使用同一有效开关的保证必须保留。若尚不能改变公开配置，先保留现状，不做表面上的 helper 改名。

**验证要求。** 单开关启停、无 exporter 的 trace ID、配置 exporter 后的初始化、HTTP middleware 与 provider 一致性、CLI/环境变量覆盖及旧配置迁移。当前四组合测试只证明现状，不代表单开关方案已验收。

## O21｜已迁移的 HTTP 业务条件仍被旧单测保活

**证据与额外成本。** [create-and-exec handler](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/interfaces/api/application_lifecycle.go#L29-L51) 已调用共享 `service.ExecuteCreateAndExecApplication`，但同文件仍留有 `shouldMarkCreateAndExecDeploying`。全仓符号检索显示它只有定义与 [旧单测调用](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/interfaces/api/workflow_create_exec_test.go#L291-L340)，没有生产调用。实际负时间拒绝与部署状态标记位于 [共享服务](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/domain/service/create_and_exec.go#L52-L73)。维护旧 helper 的测试无法约束生产规则，却让代码继续保留第二份时间条件。

[application_status.go 的聚合包装](https://github.com/PixelCores/Eruun/blob/0984611debb2cc299fdfba186f5eb73ae3993d3f/pkg/apiserver/interfaces/api/application_status.go#L70-L79) 也仅由旧测试使用或已无调用，实际 endpoint 使用同文件中的 `applicationstatus.Service`。这里的问题是迁移后的残留入口，不能推导成“生产行为没有测试”；现有 endpoint 测试仍覆盖真实执行链。

**最小简化方向。** 删除无生产调用的私有业务条件，把有价值的断言迁至现行 service／共享状态函数或真实 endpoint。确需保留测试便利包装时放入 `_test.go`，不再作为生产入口存在。不扩大为按引用数量自动删测试或导出 API 的清理。

**必须保留与验证。** 未来／过去／当前／负执行时间、task ID 为空、执行失败、状态优先级、临时失败平滑和响应脱敏。保留 `TestCreateAndExecApplicationsEndpoint*` 等真实链路测试；不能只证明删除后编译成功。

## 不列为本轮整改项的复杂性

- Workflow 数据库 lease、generation/token fencing、outbox、延迟检查点、重试身份与恢复扫描承担当前可靠性契约；减少这些机制会改变故障语义。
- adopted snapshot/签名/UID、StatefulSet 清理因果图及 PVC 删除保护解决资源身份与数据安全问题。O18 只收敛重复规则，不删除保护。
- Redis 与 Kafka 都有实际装配路径；Kafka 连续 offset 提交与 pending 状态承担并发 ACK 语义，locker 的 Lock/TryLock/Extend 也有真实消费者。
- Harbor claim、Sandbox 生命周期、恢复点和多目的结果保存有当前需求。artifact source 与数据库副本有不同保留生命周期，不能只因存在副本就定为冗余。
- HTTP/gRPC 的 ProtoJSON 转换承担 int64 与未知字段协议，账户 store 包装承担空间授权；接口或适配层数量本身不足以证明过度设计。
- `custom` CloudJob 是文档明确的源码接入模板，未作为内置 provider 注册。资源命名纯转发包装、Instant/Scheduled 的局部重复可以随邻近修改收敛，收益不足以支撑另一轮大型重构。

## 历史审计关系

本轮重新获取远端 main，并核对代码及提交历史。当前基线已包含 `02503bd`（#117）和 `5c7d41d`（#118）；旧报告顶部的“分支实施、尚未声明合入”是当时快照，不代表本轮 main 仍未修复。

| 历史范围 | 当前基线核对结果 |
| --- | --- |
| O01–O07 | Redis 装配、按角色接口、延迟通知载荷、Workflow handler、Worker 与 JSONStruct 等已处置，不重复列入 |
| O08–O12 | 校验复用、DSN、CloudJob 检查点和错误处理等整改已进入 #117 |
| O13–O15 及上轮候选 | Ingress 默认规则、旧结果发送入口、无用依赖参数、Informer workqueue 与策略常量整改已进入 #118；当前 tip 另含 #103 的 Ingress pathType 归一化修正 |

历史依据见 [2026-09-29 审计](overdesign-audit-2026-09-29.md)，其中引用的旧提交继续用于追溯，不作为本轮问题仍然存在的证据。

## 分支整改结果

| 编号 | 状态与实现 | 回归证据 |
| --- | --- | --- |
| O16 | 已实施。builder、JobInfo、执行器和 adopted manifest 统一为 `*corev1.Service`，删除 ApplyConfiguration 转换和 metadata 特判。保留同类型内的执行缺省值处理、CRUD 与冲突重试 | 生成器、Create/Update/no-op、metadata、adopted UID/重建、清理路径；新增旧 snapshot 不覆盖 live NodePort/AppProtocol、缺省值与输入不被修改的测试 |
| O17 | 已实施。转换器将每个组件与原始输入对象关联，import 在复制输入时保存 resource key，去重后直接生成资源映射与共享标记；删除 JSON 签名预算和镜像转换排序 | 同名跨 Kind、本地/共享输入同名同内容、输入换序、过时推测名称、Service/RBAC、VCT skip 与 warning 保持。保留独立转换的校验/诊断职责 |
| O18 | 已实施。`domain/model` 集中 cleanup 引用规范化、未知引用和环检测；两个调用方继续分别判断成功、覆盖范围和多个 resolver | 两侧对称测试覆盖空白/空值/重复/自引用/未知引用/环、时钟与显式因果、已完成目标、多个 resolver、不同 namespace/PVC identity、V2/V3 |
| O19 | 已实施。使用现有 `InQueryOption` 显式表达 status 与完整 scheduling_reason；保留读后检查、执行身份、CAS 和分页 | 真实 MySQL 方言 SQL 构造验证类型集合、两项等值条件、无 LIKE、第二页 offset 和 100 条上限；既有 Job/CronJob 身份保护测试保留。此项没有连接真实 MySQL |
| O20 | 已实施。保留单一 `enable-tracing`，默认开启；删除 AutoTracing 字段、flag、逻辑 OR 和静态清单的旧变量 | 启停、CLI 优先级、Redis/Kafka、exporter 独立性与旧配置拒绝测试；更新中英文 README 的迁移说明 |
| O21 | 已实施。移除 HTTP 私有时间条件和状态聚合包装；四组聚合测试移至实际领域规则，时间断言调用真实 create-and-exec 服务 | 负/零/过去/当前/未来时间、调用期间到期、空 task ID/空响应、执行失败；原端点、状态优先级、临时失败平滑和脱敏测试保留 |

配置迁移：删除 `--auto-tracing` / `ERUUN_AUTO_TRACING`，将 `--enable-tracing` / `ERUUN_ENABLE_TRACING` 设置为旧两个开关的逻辑 OR 结果。默认仍为开启；旧环境变量即使为空或 `false` 也会被明确拒绝。Jaeger endpoint 继续只决定 Span 导出。详细说明见 [中文首页](../README_zh.md#配置与本地开发)。

Go 源码调用边界：`GenerateService`、`GenerateServiceFromTrait` 和 `ApplyService` 的 Service 类型签名发生变化，仓内调用已迁移；未调查仓外消费者。HTTP/JSON、持久化字段、租约/fencing 和删除身份契约未改变。没有增加依赖、配置兼容别名或发布版本。

## 审查基线验证记录

以下均为本轮基线上实际执行的**既有行为验证**，不是尚未实施的简化方案验收。使用 Go 1.27.1。

```sh
go test ./cmd/server/app ./pkg/apiserver/event/workflow/job \
  -run 'TestResolveTracingPreservesValidatedRuntimePolicy|TestGenerateService|TestApplyService|TestServiceNeedsUpdate|TestDeployServiceJobCtlRunAdopted' \
  -count=1

go test ./pkg/apiserver/event/workflow/job \
  -run '^TestCleanupRecoveredCancelled' -count=1

go test ./pkg/apiserver/domain/service/resourceimport \
  ./pkg/apiserver/domain/service/workflow \
  ./pkg/apiserver/domain/service/application \
  ./pkg/apiserver/interfaces/api \
  -run '^(TestBuildImportPlans_|TestBuildResourceComponentNameMapping_|TestEnsureSharedComponentsOnApp_|TestStatefulSetCleanupFence(UsesExplicitCausalityInsteadOfCreateTime|RejectsUnknownResolutionTask|RejectsResolutionCycle)|TestLoadPendingStatefulSetDeletion(UsesExplicitCausalityInsteadOfCreateTime|RejectsUnknownResolutionTask|RejectsResolutionCycle)|TestShouldMarkCreateAndExecDeploying|TestCreateAndExecApplicationsEndpoint(MarksDeployingForExplicitWorkflow|DoesNotMarkDeployingForDelayedExec|MarksDeployingForPastExecuteAt|InvalidExecuteAtDoesNotExecOrMark)|TestAggregateApplicationStatus)' \
  -count=1 -v
```

上述定向测试通过；第三条命令使用临时 `GOCACHE`，4 个包的 25 个顶层测试通过、无跳过。取消恢复测试使用替身，不能证明 O19 的 SQL 筛选正确。文档交付另检查新增链接、固定提交行号、敏感内容及 `git diff --check`。

以上命令是最初审计提交的历史记录，其中旧 helper 测试已随整改迁移；不能用这组旧结果替代修复后的验证。

## 整改验证记录

使用 Go 1.27.1，以下检查通过：

```sh
go test -race -cover -p 2 ./...
go vet ./...
go build -trimpath -o /tmp/eruun-overdesign-fix-server ./cmd/main.go
go test -tags=integration -run '^$' ./...
deploy/all_in_one_install_quickstart_test.sh
scripts/check-sensitive-content.sh
git diff --check
```

Go 命令使用可写临时 `GOCACHE`；全仓 race/coverage 中 54 个测试包通过，另有无测试文件或仅统计覆盖率的包。integration 命令只验证带标签的测试可编译，没有执行集成测试。全仓格式检查无输出。最后的 Service 缺省值、旧快照字段保护及摘要边界另经 job/workflow 定向 race 测试通过。四组状态聚合测试已逐组比对，原用例和断言完整迁移到实际领域规则。

本轮未修改 Chart，未执行 Helm 模板检查（本机无 Helm），也未构建容器镜像。

真实 MySQL 查询执行、Kubernetes API server 默认化、Redis/Kafka、Jaeger 导出、云服务、负载与仓外 Go 消费者未验收；fake client 与 SQL DryRun 不构成真实服务验收。远端检查结果以 PR 当前 head 为准。
