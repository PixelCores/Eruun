# Go 惯用风格与过度设计复审（2026-09-29）

> 状态：Historical / Audit。代码基线为 `main` 的 `ae9306fc816e9e9845a16dd7bcefd91542bb2e6d`。本文是供逐项评审的设计审计，不改变运行行为；路径和行号均针对该提交。Draft / Proposal 不作为已实现契约。

## 结论与判断标准

本次审查覆盖启动和配置、HTTP/gRPC、领域服务与仓储、Workflow/Job/Traits、Harbor 评测、基础设施以及部署。最明确的行为问题是 **Ingress API 摘要与实际 Kubernetes 资源使用两套路径类型解析规则**：同一输入可得到不同的 `pathType`。其余已确认项主要是当前没有消费者的接口和配置、部分初始化的 service、重复分发或隐式依赖传递；它们是维护成本，不据此推断线上故障或性能收益。

判断“过度设计”要求同时看到当前调用链和具体代价：没有生产消费者、同一规则被重复实现且发生分叉、公开配置承诺无法兑现，或者抽象使必需依赖只能在运行时才发现。文件长、接口多、使用泛型或存在多种实现，本身不足以定性。简化应维持现有 HTTP/gRPC 字段、错误、持久化和 Kubernetes 资源身份；不为替换一个抽象再增加新的通用框架。

| 审查面 | 已核对的入口与边界 | 结果 |
| --- | --- | --- |
| 启动、配置、部署 | `cmd/server/app` → `config.Validate` → `server_assembly`；Helm、静态清单、日志 | R08–R10；日志默认值仅作条件候选 |
| HTTP/gRPC、领域层 | API DTO/assembler → service → repository；gRPC 的当前双入口 | R01–R04、R11；保留公开协议 |
| Workflow/Job/Traits | Controller → `RunJobs` → 具体 JobCtl、Ingress Trait、CloudJob、命名 | R01、R05–R07、R12；租约和恢复不在删除范围 |
| 独立 Jobs、Harbor | Job 状态、Runner、制品/交付与 Sandbox 生命周期 | 未发现支持删除结果门禁或恢复机制的证据 |
| 测试与文档 | 现行文档、历史审计、相关测试和仓内调用搜索 | 标明下列验证边界，不把历史结论当当前待办 |

这是按主要执行链与风险边界进行的全仓审查，不是逐行形式化证明。仓内“无调用”不证明仓库外 Go 消费者不存在；发布删除导出符号的 PR 前仍须查询下游。未做性能基准、真实 MySQL/Redis/Kubernetes 故障注入或部署验收。

## 与上一份审计的关系

[上一份全仓审计](overdesign-audit-2026-09-29.md)冻结在 `9097b26`。在本次基线之前，O01–O07 对应的改动已经分别进入 `main`，不能再当作当前未修问题：O01 的 Redis 缓存配置由 #97 收敛，O02 的角色适配器由 #95 收敛，O03 的延迟通知由 #101 改为身份消息，O04 的应用消费接口由 #98 调整，O05 的锁构造由 #96 收敛，O06 的 Worker 订阅契约由 #99 收敛，O07 的旧 JSON 转换入口由 #100 删除。本报告沿当前代码重新取证；CloudJob 的旧“工厂候选”仅在 R07 中补充实际的双重分发链。

## 当前代码发现与简化机会

| 编号 | 影响 | 当前证据与简化目标 |
| --- | --- | --- |
| R01 | P2，已复现行为偏差 | Ingress 摘要与资源生成各自解析 `pathType`；统一规范化规则 |
| R02 | P3，死抽象 | `WorkflowQueueRepository` 装配后无生产方法调用；删除空包装 |
| R03 | P3，隐式依赖 | 跨包辅助函数临时构造部分初始化的应用 service；显式传递所需依赖 |
| R04 | P3，额外维护面 | 缓存接口三个方法只有测试消费者；按真实消费者缩窄 |
| R05 | P3，依赖传递成本 | `RunJobs` 长参数串和 JobCtl 后注入；收敛运行依赖传递 |
| R06 | P3，类型擦除 | 资源生成结果使用 `interface{}`，清理路径再次断言；逐步恢复静态类型 |
| R07 | 待验证的设计候选 | 内置 CloudJob 对相同 action 先查工厂、再 switch；先核定扩展契约 |
| R08 | P3，无效条件 | 合法消息后端使“自动追踪”条件恒成立；明确启用策略 |
| R09 | P3，错误配置表达 | flag 宣称 TiDB，运行装配和迁移只支持 MySQL；修正文案与校验 |
| R10 | P3，死配置 | 三个 Config 字段仅声明或赋默认值；删除前核查外部构造 |
| R11 | P3，特殊装配 | 编程语言 repo/service 显式构造后又经两个可变参数 override 注入；恢复单一路径 |
| R12 | P3，未用分支 | 运行时命名函数的版本分支没有仓内生产调用；维持现有资源名后收窄参数 |

### R01｜Ingress 路径类型规则重复并已产生偏差

**证据。** API 的 [`normalizeIngressPathType`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/interfaces/api/assembler/v1/component_service.go#L283-L310) 先 `TrimSpace`；Kubernetes 资源生成的 [`parsePathType`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/workflow/traits/ingress.go#L361-L389) 不做同样处理。以 `defaultPathType: " Exact "`、路由未单独指定 `pathType` 为输入，在相同 Ingress spec 上分别调用 `ConvertComponentModelToDTO` 和 `BuildIngress`，本地探针输出 `write-validation=<nil>`、`api=Exact k8s=Prefix`。[组件准备](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/application.go#L892-L937)将请求 Traits 序列化到模型，随后[组件写入](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/application.go#L420-L449)持久化；[写入校验入口](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/application.go#L966-L973)及其[Ingress 规则](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/internal/traitvalidation/trait_validation.go#L159-L188)均未检查 `pathType`。探针验证了写入校验和两个生成函数，未执行完整 HTTP→DB→Kubernetes 流程。

**简化与验收。** 在已有 `domain/spec` 中确定一次规范化与无效值语义，让 API 组装和 Trait 构建复用；不要让领域包反向依赖 Kubernetes 类型。先加同一 Traits 输入的跨层表驱动测试，覆盖大小写、空白、显式 route 值、trait 默认值和 regex fallback。是否拒绝原本被默许的无效值属于单独的 API 契约决定，不应混在纯重构中。

### R02｜没有生产消费者的 Workflow Queue 仓储接口

**证据。** [`WorkflowQueueRepository`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/repository/workflow.go#L128-L172) 有六个方法，全部只转发给同包已有的带 `DataStore` 参数函数。启动会[注册](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/repository/init.go#L3-L17)，应用 service 也有[注入字段](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/application.go#L104-L118)，但对 `WorkflowQueueRepo` 的仓内非测试引用仅见字段、接口和装配，没有方法调用；队列的实际逻辑直接调用同包 typed 函数。测试却需要[六方法假仓储](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/test_helpers_test.go#L351-L399)。

**简化与验收。** 删除未用字段、注册、转发接口及仅测试转发的断言；保留被生产代码使用的 typed 函数。确认没有仓外 Go 调用，并运行应用生命周期和 Workflow Queue 的针对性测试；不要把事务内可用的 `DataStore` 调用一并删除。

### R03｜部分初始化的应用 service 被当作跨包工具对象

**证据。** [`application/shared_exports.go`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/shared_exports.go#L14-L69) 在组件解析、资源名校验、回调校验等辅助入口分别建立只填少数字段的 `applicationsServiceImpl`，再调用 receiver 方法。[Validation](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/validation/validation.go#L165-L179) 等相邻服务依赖这些导出入口。这些包装函数虽列出当前参数，借用的大型 receiver 却允许内部方法新增字段而不触发编译错误，缺失依赖可能直到执行时才暴露。这里有实际跨包消费者，因此不能简单删除。

**简化与验收。** 对确实共享的领域规则使用所属包内的函数和显式参数；需要 service 状态的行为仍由正常装配后的应用 service 执行。按一条链路迁移，比较 Try/Create 的模板展开、资源命名、空间授权和回调 URL 结果；不要将业务规则搬进 `utils` 或制造新的通用 usecase 层。

### R04｜缓存接口维护无生产调用的方法

**证据。** [`ICache`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/infrastructure/cache/icache.go#L11-L21) 的 `Consume`、`List`、`Exists` 仅在缓存测试中调用；Redis 与内存实现都维护它们。Redis 的 [`List`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/infrastructure/cache/redis_cache.go#L64-L123) 包含 SCAN、MGET 和独立超时，但没有当前服务消费者。另一个导出类型 [`RedisCache`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/infrastructure/cache/redis_cache.go#L11-L19) 在仓内也没有构造调用。

**简化与验收。** `ICache` 保留实际需要的 Store/Load/Delete/IsCacheDisabled；测试改为验证消费者行为，而不是要求无消费者方法存在。查外部 Go 消费者后再删除导出入口。此项不等于删除测试用 `MemCache`，也没有测得 Redis 性能瓶颈。

### R05｜Job 运行依赖由长参数串和后注入传播

**证据。** [`RunJobs`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job.go#L275-L276) 接受 14 个固定参数和可变 keyring，Controller 多处重复转发。Job 包内已有 [`jobRuntime`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job.go#L74-L106) 来聚合依赖，但 `initJobCtl` 创建具体控制器后仍通过匿名 [`setRuntime` 断言](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job.go#L240-L255)选择性注入。新 JobCtl 若忘记实现这个非公开约定，缺失能力只能在执行中暴露。

**简化与验收。** 在现有 Job 边界收敛依赖传递，并使确实需要 runtime 的控制器在构造时明确接收它；不要再叠加一个可选注入接口。分 Job 类型验证直接构造、取消、失败状态持久化、并发运行和回收；保持当前资源权限与租约语义。

### R06｜资源生成结果擦除类型后再做断言

**证据。** [`GenerateServiceResult.Service`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job.go#L61-L64) 是 `interface{}`，而其 `AdditionalObjects` 已是 `[]client.Object`。清理路径按组件类型再次断言 Deployment、StatefulSet、Job、CronJob（[实际消费](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job_cleanup_resources_generated.go#L22-L107)）。一个泛化的空接口并没有消除分支；清理处断言失败会静默退回按组件推导的名称，而非由编译器保证生成类型。

**简化与验收。** 至少先限制为 Kubernetes `client.Object`，再按生成函数的真实返回类型逐步收窄结果；不要为了消除 type switch 创造新的资源描述语言。比较生成/清理的名称、Namespace、附属对象、缺失对象处理与存量资源归属。

### R07｜内置 CloudJob 的第二次字符串分发需验证收益

**证据。** `CloudJobCtl` 先按字符串[查找 action](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job_cloud.go#L92-L106)；Aliyun provider 再经[工厂表](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L34-L45)创建无状态 action；[action 再调用 `runtime.Call`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/cloudjob/aliyun/action_nas_ensure_filesystem.go#L31-L52)，最终在 [`client.Call` 的 switch](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/cloudjob/aliyun/client.go#L51-L74)按同一字符串再次分发。当前生产[注册](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/cloud_provider_bridge.go#L9-L18)只有 Aliyun，自定义 provider 模板未注册。

**简化与验收。** 当前[CloudJob 实现参考](cloudjob-skeleton.md)明确 provider/action/runtime 分层，可替换的 `CloudRuntime` 也有测试用途；双重字符串分发本身尚不足以证明过度设计。保留 provider 选择、action 进度、检查点、状态拷贝与恢复契约，先用一项内置 action 评估能否收敛第二次字符串分发，并核对扩展模板及 fake runtime 测试。没有基准证明性能损失，不建议跨 provider 一次性重写。

### R08｜自动追踪的后端判断没有实际选择作用

**证据。** 正常命令在 `Run` 前执行 [`Validate`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/cmd/server/app/server.go#L43-L50)，[消息校验](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/config/config.go#L304-L337)只接受 Redis/Kafka；这两者都使 [`hasExternalQueue`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/cmd/server/app/server.go#L199-L210)为真。因此合法常规启动中 `effective` 等价于 `EnableTracing || AutoTracing`，Jaeger endpoint 不影响自动启用条件。默认值是前者 true、后者 false。

**简化与验收。** 先明确“自动”在当前产品中究竟要表达什么，再收敛内部判断或公开 flag；`--auto-tracing` 已是配置入口，不能静默改变旧部署。测试四种 flag 组合和有/无 exporter 的 trace ID 行为，不把 tracer provider 初始化误当成 exporter 已交付。

### R09｜数据库后端 flag 承诺了未装配的 TiDB

**证据。** [`--datastore-type` 帮助](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/config/config.go#L359-L360)列出 `mysql, tidb`，配置还保留 `TIDB` 常量；[运行装配](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/server_assembly.go#L79-L92)与[迁移模式](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/schema.go#L12-L25)均只接受 MySQL。`Validate` 的 DSN 检查仅针对 MySQL，TiDB 会到后续阶段才报不支持。

**简化与验收。** 让帮助和前置校验表达当前 MySQL-only 事实；保留 `--datastore-type` 本身可避免无关的配置破坏。只有在真实 TiDB 装配、迁移和测试齐备后才宣传支持。检查启动和 migrate-only 的错误文本。

### R10｜仅声明或赋默认值的旧 Config 字段

**证据。** `DTMAddr`、`IstioEnable`、`AddonCacheTime` 见 [`Config` 声明](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/config/config.go#L55-L84)和[默认赋值](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/config/config.go#L191-L199)，仓内非测试代码没有读取或 flag 绑定。它们暗示存在 DTM、Istio 或缓存刷新策略，却不控制运行行为。

**简化与验收。** 从 Config 及默认值中删除，检查仓外 Go 字段访问；不要增加兼容别名或假开关。配置说明应只描述实际生效项。此项不会替代对未来 Proposal 的单独设计。

### R11｜编程语言服务走独有的双重装配路径

**证据。** [`provideDomainAndEventBeans`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/server_assembly.go#L204-L225)先显式构造编程语言 repository/service，再把同一实例传给 [`InitRepositoryBean`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/repository/init.go#L3-L17) 和 [`InitServiceBean`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/service.go#L70-L91) 两个可变参数 override。其他同类对象经单一装配路径进入容器；此处需要同时理解提前校验、override 和后续 `Populate`。

**简化与验收。** 先证明现有 IoC 能对该服务做等价的 fail-fast 依赖校验，再选择单一路径并移除两个 override 分支。覆盖 HTTP/gRPC 编程语言 CRUD、缺依赖启动失败和实例唯一性；不因减少代码行而放宽失败语义。

### R12｜运行时命名函数保留未使用的模板版本分支

**证据。** [`ApplicationResourceKey(appName, version, templateEnabled)`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/workflow/naming/naming.go#L57-L70) 对 `templateEnabled=true` 拼接版本；六个仓内非测试调用一律传 `"", false`，例如 [应用](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/application/application_resource_naming.go#L14-L19)和 [Job Builder](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job_builder_inputs.go#L153-L159)。当前注释也说明运行时命名不带模板版本。

**简化与验收。** 核查仓外调用后，把运行时命名入口收为所需的应用名；若以后有真实 catalog 消费者，再在其所属层表达 catalog key。用现有资源名测试逐项比对输出，尤其避免改变 Kubernetes 资源身份和清理定位。

## 需要先定契约或补证据的候选

| 候选 | 已看到什么 | 决策与验证门槛 |
| --- | --- | --- |
| 转换 API 的验证依赖 | [`ConvertKubeResources`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/domain/service/conversion/conversion.go#L73-L101) 默认 `validate=true`，但依赖为 nil 或 `TryApplication` 返回 nil 时仍保留 `Valid:true`。正常 IoC 会注入，尚无生产可达证据。 | `validate=true` 时缺依赖/空结果宜显式失败；保留用户明确的 `validate=false` 语义，补两种负例及启动装配测试。 |
| 异步日志归档 | Workflow 仍可[构建 `log_archive_upload`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job_builder_component.go#L326-L365)，但生产代码未调用 [`SetArchiveUploader`](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job_log_archive_upload.go#L50-L68)，在 `kubeConfig` 已正常注入时，执行会[报未配置错误](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/pkg/apiserver/event/workflow/job/job_log_archive_upload.go#L136-L142)；[当前文档](log-archive-upload-workflow.md)已说明此边界。 | 先决定交付 uploader 还是废弃异步入口；后一种选择必须处理已有 workflow jobType、API 和存量任务兼容。不能直接删掉文档化分支。 |
| 容器内文件日志 | Helm [默认值](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/deploy/helm/eruun/values.yaml#L11-L14)和静态清单同时启用文件与 stderr；运行时有[清理循环](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/cmd/server/app/server.go#L84-L97)，而 Chart 没有专用日志卷。 | 先确认运维是否读取 Pod 内文件；若没有，再评估默认仅 stderr、文件写入显式启用，并同步部署测试和现行文档。 |

另外，`domain/service/service.go` 的类型别名和简单构造转发、唯一调用的 `convertDTOList`、固定 `/api/v1` 却返回 `[]string` 的 `GetAPIPrefix` 都可在邻近改动时收敛。它们已有消费者或收益很小，不建议为此单独开启全仓机械迁移。

## 应保留的复杂度

- Workflow 的数据库执行租约、generation/token fencing、延迟检查点与恢复、结果 outbox 分别保障跨实例执行归属、延迟恢复和结果可靠交付；R05–R07 不要求删除这些状态机。参见[当前运行时参考](enterprise-distributed-runtime-design.md)与[Workflow 指南](workflow-architecture-guide.md)。
- Harbor 的 Sandbox 申请、Runner claim 与终态、制品及结果交付是当前评测契约；不能因为代码量大而省略结果门禁或降级为进程内状态。参见[Workspace Jobs API](workspace-jobs-api.md)。
- HTTP 与 gRPC 双入口是[当前公开协议](grpc-api.md)，Protobuf 生成代码不应计作手写抽象债务。类型、optional 和数字字段转换需保持协议一致。
- Helm 对 StatefulSet、Secret、PVC 身份的检查，以及安装脚本的凭据复用和 `0600` 临时文件，是持久数据与密钥保护；不能为了缩短脚本删除。参见[部署说明](helm-deployment.md)。

## 处置顺序与验证记录

1. **先修已复现偏差。** R01 单独一个小 PR，加入跨层同输入测试，再统一规则；不要同时调整无效值契约。
2. **再删没有消费者的表面。** R02、R04、R10、R12 可分别在所属包做窄 PR；每项先核查导出 Go 符号的外部使用与现有测试替身。
3. **按调用链收敛隐式依赖。** R03、R05、R06、R11 分开实施，保持失败、事务、状态和 Kubernetes identity；R07 先核定 provider 扩展边界。
4. **公开配置与产品决策单列。** R08/R09 需要配置帮助、测试和文档同改；三个条件候选先取得可达性或运维证据。

本次用 `rg` 核对当前源码及非测试消费者，并读取相关 Current 文档和旧审计。针对 R01，以临时的仓外 Go 探针在 Go 1.27.1 上执行 `ConvertComponentModelToDTO` 与 `BuildIngress`，得到 `write-validation=<nil>`、`api=Exact k8s=Prefix`；探针未加入仓库。`GOCACHE=/tmp/eruun-go-build go test ./pkg/apiserver/workflow/traits ./pkg/apiserver/interfaces/api/assembler/v1` 通过；现有测试未覆盖这个空白输入的跨层差异。本文仅修改文档，未运行全仓 Go suite、race、真实数据库/集群或性能基准；上述运行时边界仍需在各自实施 PR 中验证。
