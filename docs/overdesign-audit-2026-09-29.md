# 全仓过度设计审计（2026-09-29）

> 状态：Historical / Audit。审查基线为 `main` 的 `9097b26c101532604b36d011aa53cec37857ee72`。本文只分析当前实现，不改变运行行为，也不把 Draft / Proposal 当作已实现契约。代码位置均以该提交为准；后续改动应重新核对。

## 范围与判断方法

本次从 `docs/README.md` 路由到启动与配置、HTTP/gRPC 和领域服务、Workflow/Job/Traits、Jobs/Harbor、基础设施、Helm 与安装脚本，沿实际装配点和调用链抽查。重点是**当前需求用不到，却已经增加配置分支、持久化副本、注入负担或维护表面**的设计。没有逐行检查所有文件，也没有做性能压测、真实 MySQL/Redis/Kubernetes 故障注入或外部 Go 包消费者调查。

下文已确认的 O01–O07 均是 **P3 维护债务或配置表达问题**，不据此宣称已发生线上故障。CloudJob 与小型抽象另列为待取证或随手收敛的候选。建议顺序考虑可达性、收益和改动风险，而不以文件长度、接口数量或抽象层数单独定性。

| 范围 | 核对重点 | 结论 |
| --- | --- | --- |
| `cmd/`、`config/`、`server*` | 角色启动、配置校验、IoC 与路由装配 | O01、O02；未建议全仓更换 IoC |
| `interfaces/`、`domain/` | HTTP/gRPC 消费接口、DTO 转换、模型转换 | O04、O07；保留公开 API 和 JSON 契约 |
| `event/workflow/`、`workflow/traits`、`jobs/` | 延迟任务持久化与通知、Worker、CloudJob、Runner | O03、O06 及 CloudJob 候选；恢复与 fencing 有现行需求 |
| `infrastructure/`、`deploy/` | 缓存、锁、消息探针、Helm 与安装 | O01、O05；未发现足以支持删除安全部署约束的证据 |

## 已确认的简化机会

### O01｜缓存后端存在运行时不可达的选项（P3，建议先处理）

**证据。** `--cache-type` 的帮助文字写着 `redis|memory`（[配置注册](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/config/config.go#L380-L388)），但普通启动的 `Validate` 对所有非 Redis 值报错（[配置校验](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/config/config.go#L258-L263)）；命令入口在 `Run` 前调用该校验。服务装配仍保留 `memory` 分支（[server_assembly.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server_assembly.go#L111-L135)），缓存包还有后端选择工厂及 Redis 客户端为 nil 时回退内存的构造路径（[icache.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/cache/icache.go#L36-L57)、[redis_cache.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/cache/redis_cache.go#L31-L51)）。迁移专用模式虽跳过该检查，却不会装配运行服务。

**代价与方向。** 用户看到不可用的运行选项，维护者还需理解多套实际上到不了的 fallback。按当前分布式锁和认证必须使用 Redis 的契约，收敛帮助文字、运行装配和无生产调用的选择工厂；保留 `NewMemCache` 作为测试替身。Redis 初始化失败仍应明确失败，不应静默退到进程内缓存。若产品确需支持内存模式，应先定义多副本互斥和认证行为，再选择实现，不能只放宽校验。

**验证边界。** 配置拒绝 `memory`、Redis 缺失启动失败、缓存单测以及使用内存替身的服务测试。已有 `TestValidateApplicationMutationLockRequiresRedisCacheType` 可作为现状基线。

### O02｜非 API 角色仍构造并注入业务接口适配器（P3，建议先处理）

**证据。** `New` 无条件创建整套 HTTP handler（[server.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server.go#L105-L112)）。`provideDomainAndEventBeans` 随后无条件注入全部 handler 和三个 gRPC 业务适配器（[server_assembly.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server_assembly.go#L235-L246)）。然而 Controller、Scheduler、Worker 仅注册 health 路由，且只有 API 角色启动 gRPC（[server_bootstrap.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server_bootstrap.go#L222-L230)、[同文件](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server_bootstrap.go#L314-L322)）。

**代价与方向。** 非 API 进程承担不提供服务的适配器构造和反射注入；业务适配器新增依赖时也会扩大其他角色的启动约束。让 API 角色装配业务 HTTP/gRPC 适配器，其他角色仅装配 health 所需对象；领域和 Event 依赖仍应按各自真实消费者保留。先以角色装配测试确认依赖图，不要求同时替换全仓 IoC。

**验证边界。** 四种角色分别启动并验证路由、gRPC 监听、health/readiness 和依赖失败行为；尤其不能把 Worker 或 Controller 实际使用的领域服务误删。

### O03｜延迟 Job 在数据库和通知队列保存两份完整工作负载（P3，收益需测量）

**证据。** `DelayJobPayload` 含完整 `*batchv1.Job`（[delay_queue.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/delay_queue.go#L23-L34)），生产者先把同一 JSON 写入 `JobInfo.DelayPayload`，再发往队列（[job_scheduled.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/job_scheduled.go#L168-L185)）。Controller 能直接从数据库恢复到期检查点（[delay_dispatcher.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/delay_dispatcher.go#L321-L375)）；消费通知后再次读取数据库，并将完整 JSON 与通知副本比较（[同文件](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/delay_dispatcher.go#L506-L520)、[同文件](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/delay_dispatcher.go#L593-L607)）。[当前运行时文档](enterprise-distributed-runtime-design.md#5-job-执行身份)也明确数据库保存完整载荷，队列负责降低发现延迟。

**代价与方向。** 队列承担完整 Kubernetes Job 的序列化与传输，还需要双份内容相等的检查。可评估通知只携带执行身份或检查点 ID，消费端始终读取数据库中的完整载荷；这使通知含义更贴合现有事实源。此处只确认冗余路径，未测得延迟、流量或存储收益。

**验证边界。** 保留即时唤醒、丢消息及 Leader 切换后的数据库恢复、跨实例去重、generation/token 校验、CAS 和 ACK。现有消息解码要求内嵌 Job；如改变队列格式，须为旧消息安排版本化读取或协调排空，不能直接切换。

### O04｜应用服务的整块接口扩大局部消费者的替身成本（P3）

**证据。** `ApplicationsService` 声明 32 个方法（[application.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/domain/service/application/application.go#L40-L74)），HTTP `applications` handler 注入整块接口（[applications.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/interfaces/api/applications.go#L17-L24)）；一个工作流路由测试因此实现了同样 32 个空方法（[workflow_test.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/interfaces/api/workflow_test.go#L208-L337)）。这证明了实际测试维护成本，并非仅以方法数量判断。

**代价与方向。** 局部测试须跟随无关方法变动，handler 的依赖需求也不清晰。先在一个真实消费边界试点收窄接口或路由级依赖，再按收益决定是否推广；不为每个方法新增接口，也不拆散现有领域事务。需同时检查反射注入是否仍能唯一匹配。HTTP/gRPC 字段与错误契约不应因结构调整而变化。

### O05｜锁后端选择工厂包含没有生产选择路径的实现和占位（P3）

**证据。** `locker.Type` 定义 Redis、Memory、Noop、尚未实现的 etcd，`New(Config)` 按类型分支，etcd 仅返回“not yet implemented”（[locker.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/locker/locker.go#L42-L56)、[同文件](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/locker/locker.go#L100-L120)）。仓内两处生产装配都固定传入 Redis（[server_assembly.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server_assembly.go#L140-L149)、[shared_resource_locking.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/shared_resource_locking.go#L78-L91)），没有面向用户的锁后端配置。`Config.RedisClient` 为 `interface{}`，使用时才断言为 `*redis.Client`（[options.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/locker/options.go#L112-L127)、[redis.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/locker/redis.go#L205-L212)）。

**代价与方向。** 当前固定依赖被包装成运行时选择和类型断言。生产装配可直接使用现有 `NewRedisLocker(*redis.Client, prefix)`；保留 Memory/Noop 的直接构造函数供跨包测试使用。`WithMetadata` 只见测试调用，Redis/Memory 实现均不读取该字段，可在清理时一并确认（[options.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/infrastructure/locker/options.go#L49-L51)）。删除导出入口前仍需核查仓库外 Go 调用。

### O06｜Worker 类型已保证能力，却在启动时再次动态断言（P3，低成本）

**证据。** `event.Worker` 只嵌入 `WorkerSubscriber`，而 `InitEvent` 当前只返回一个 Workflow worker（[event.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/event.go#L19-L32)）。`startWorkers` 仍把每个非 nil `Worker` 断言回 `WorkerSubscriber`，失败时跳过（[server_workers.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/server_workers.go#L123-L132)）。按当前静态类型，该失败分支不可达。

**代价与方向。** 无意义的能力探测会掩盖真正的 worker 清单及 readiness 计数。直接使用 `[]event.Worker` 或将唯一有用的接口命名为 `Worker`；保留 nil 过滤与现有启动/停止语义。此项只减局部复杂度，优先级低于 O01–O03。

### O07｜`JSONStruct` 仍暴露仓内未使用的旧转换入口（P3，低成本）

**证据。** `NewJSONStruct(*runtime.RawExtension)`、`NewJSONStructByString` 和 `(*JSONStruct).RawExtension()` 在非测试代码中只有定义，没有调用（[model.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/domain/model/model.go#L20-L46)、[同文件](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/domain/model/model.go#L79-L93)）。`RawExtension()` 还将 JSON map 经 YAML 再转回 JSON。仍被实际使用的 `NewJSONStructByStruct` 已在当前代码改为直接 JSON 编解码（[同文件](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/domain/model/model.go#L49-L63)）。

**代价与方向。** 未用入口与 `runtime`/`yaml` 依赖继续扩大模型 API。确认没有仓库外 Go 消费后删除这些入口；保留 `NewJSONStructByStruct`、`Bytes` 和 `Properties`。这不是重新报告旧审计 A10：该项的核心转换路径已修复。

## 需要更多证据的局部候选

- **CloudJob action 工厂。** [运行时注册](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/cloud_provider_bridge.go#L9-L18)仅包含 Aliyun；该内置 provider 与仓库中未注册的 Custom 扩展模板通过 `CloudActionFactory` 每次创建 action（[contracts.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/cloudjob/contracts/contracts.go#L53-L60)、[aliyun/provider.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L122-L131)、[custom/provider.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/cloudjob/custom/provider.go#L34-L43)）；当前 action 构造函数只返回空结构体，例如 [NAS action](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/cloudjob/aliyun/action_nas_ensure_filesystem.go#L11-L15)。内置实现或可直接保存 action，但自定义 provider 的实例状态需求尚未核定，不建议直接删公共工厂契约。
- **CloudJob context 中的必需依赖。** `CloudJobCtl.Run` 把 datastore 放进 context，Aliyun provider 再取出，缺失时运行时失败（[job_cloud.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/job/job_cloud.go#L61-L75)、[aliyun/provider.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L51-L68)）。显式执行输入或构造注入可让依赖可见；但必须先核对自定义 provider、恢复快照和凭据不落库的边界。
- **小型单用途抽象。** `convertDTOList` 只被一个 HTTP handler 调用，却有两个类型参数、转换函数和错误回调（[application_workflow.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/interfaces/api/application_workflow.go#L28-L57)）；固定 `/api/v1` 仍通过可变变量和 `[]string` 供两处循环（[interfaces.go](https://github.com/PixelCores/Eruun/blob/9097b26c101532604b36d011aa53cec37857ee72/pkg/apiserver/interfaces/api/interfaces.go#L5-L10)）。两者可随邻近代码改动时收敛，不值得单独启动大规模重构。

## 保留的复杂性与历史审计关系

- Workflow 的数据库租约、generation/token fencing、结果 outbox、延迟检查点、Kubernetes 资源归属和空间授权有当前正确性需求。O03 只讨论通知是否需携带完整载荷，不建议去掉检查点、恢复扫描或一致性保护。
- Kafka readiness 的端到端探测有[当前设计说明](kafka-queue-implementation.md)；尚无探针成本或故障数据支持将其列为确定过度设计。Harbor Runner 的结果保存、Sandbox 生命周期和阶段事件同样有现行契约。
- 旧[代码质量审计](code-quality-audit-2026-09-26.md)记录的是 `d075a82`。其主要修复已经随 #85–#92 进入本次 `main` 基线；本报告没有直接沿用旧 A01–A13，也没有把已改成 JSON 的结构体转换、实例化 handler 或显式 Redis 装配再次算作未修问题。

## 验证记录与建议顺序

- 已执行 `go test ./pkg/apiserver/config -run TestValidateApplicationMutationLockRequiresRedisCacheType -count=1`：通过，确认当前启动校验的相关测试。
- 已执行 `go test ./pkg/apiserver/event/workflow/job -run 'TestPersistDelayJobCheckpointStoresRecoverablePayload|TestDelayDispatcherRecoversDueCheckpointWithoutQueue' -count=1`：通过，确认数据库检查点和无队列恢复的现有测试。
- 本次是文档审计，没有改动 Go、配置、镜像或部署行为；未运行全仓测试、race/coverage、真实服务部署或性能基准。代码搜索中的“未使用”仅针对仓内非测试调用，不能证明仓库外没有依赖。
- 建议先收敛 O01 的公开配置表达和 O02 的角色装配；再以旧消息兼容测试与测量结果推进 O03。O04–O07 可按各自消费边界做小 PR，避免把行为变更与全仓结构调整混在一起。
