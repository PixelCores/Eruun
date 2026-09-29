# 代码质量审计：复杂度、Go 惯用法与抽象边界（2026-09-26）

> 状态：Historical / Audit。本文记录 `main` 在 `d075a82db6b8a9f1dada57a86920321ed8fd4147` 的代码快照；问题和建议尚未实施，不代表最新运行状态或新的产品契约。

## 审查结论与边界

当前较值得处理的问题是：错误在跨层调用中丢失、同一业务存在语义不同的提交实现、固定类型关系被隐藏在反射或运行时断言中，以及读写流程为了复用展示层而绕行。它们比统一命名或继续拆文件更值得优先处理。

本次沿 HTTP/gRPC、领域服务、Workflow/Job、Traits、Jobs、datastore/cache 和 server 装配检查实际调用链，选择以下 13 项有具体证据的问题。对 account、resourceimport、Harbor 和其他基础设施作了定向抽查；这不是逐行穷尽检查，也不是安全认证或性能压测。生成的 Protobuf 代码、第三方实现及尚未实现的 Proposal 不作为设计质量指控对象。

- **行为缺陷**：当前函数契约或控制流已能证明问题；没有集群/故障注入证据的影响明确写成触发条件，不宣称发生过生产事故。
- **维护债务**：有实际重复、依赖泄漏或类型约束缺失，但不据此推断正常部署一定出错。
- **P2**：有界行为缺陷，建议优先修复；**P3**：维护债务，适合随所属模块的小 PR 简化。优先级不等于故障发生频率。
- 分类中的 **复杂度** 对应不必要的绕行/重复设计，**Go** 对应错误、context、类型等惯用法，**抽象** 对应职责、依赖和生命周期边界；一项可属于多类。

每项代码链接固定到上述提交，避免后续行号漂移。本文只提出局部改进方向，不授权一揽子重构。

## 问题总表

| 编号 | 优先级 / 性质 | 分类 | 问题 |
| --- | --- | --- | --- |
| A01 | P2 / 行为缺陷 | Go、抽象 | 资源生成失败用 nil 表达，调用者把它当作没有任务 |
| A02 | P2 / 行为缺陷 | 复杂度、抽象 | 版本更新的重复提交路径具有不同原子性 |
| A03 | P2 / 行为缺陷 | Go | DBError 包装后丢失标准错误链 |
| A04 | P2 / 行为缺陷 | Go、抽象 | 未命名 init/sidecar 每次渲染生成随机名称 |
| A05 | P2 / 行为缺陷 | Go、抽象 | 缓存接口不能传递调用者的取消和截止时间 |
| A06 | P3 / 维护债务，失败处理差异已确认 | 复杂度、Go | 操作任务存在两套 writer，子记录失败语义不一致 |
| A07 | P3 / 维护债务 | 复杂度、抽象 | 全局 API registry 保存属于 server 的可变 handler 实例 |
| A08 | P3 / 维护债务 | 复杂度、Go、抽象 | 固定 Trait 集合通过字符串、反射与类型断言调度 |
| A09 | P3 / 维护债务 | 复杂度、抽象 | 领域规格转换绕经完整展示 DTO，并反向依赖接口层 |
| A10 | P3 / 维护债务 | 复杂度、Go | JSONStruct 的结构体转换绕行 YAML |
| A11 | P3 / 维护债务 | Go、抽象 | Jobs 的必需存储能力仍靠“可选接口”运行时断言 |
| A12 | P3 / 维护债务 | 复杂度、抽象 | execution 解析在多个 helper 中重复全量查询 |
| A13 | P3 / 维护债务 | 抽象 | ICache 同时充当 Redis 依赖入口 |

## A01：资源生成失败用 nil 表达，调用者把它当作没有任务

**代码证据。** [`pkg/apiserver/event/workflow/job/job_deploy.go:L325–L333`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job/job_deploy.go#L325-L333) 和 [`pkg/apiserver/event/workflow/job/job_instant.go:L36–L39`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job/job_instant.go#L36-L39) 在 `ApplyTraits` 失败后只写日志并返回 `nil`。[`pkg/apiserver/event/workflow/job_builder_component.go:L647–L679`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job_builder_component.go#L647-L679)、[`pkg/apiserver/event/workflow/job_builder_component.go:L704–L725`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job_builder_component.go#L704-L725) 收到 nil 后直接不添加 workload Job。[`pkg/apiserver/event/workflow/job_builder_steps.go:L230–L248`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job_builder_steps.go#L230-L248) 跳过空 bucket，[`pkg/apiserver/event/workflow/controller.go:L490–L500`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/controller.go#L490-L500) 在无剩余步骤时进入成功路径。

**触发与代价。** Trait 聚合会拒绝同 identity、不同内容的附加对象（[`pkg/apiserver/workflow/traits/processor.go:L287–L304`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/processor.go#L287-L304)）。主容器和 sidecar 为同名 PVC 声明不同规格，就是一个真实错误输入。生成器把错误转为 nil 后，执行器无法区分“合法无任务”和“渲染失败”；最终可能漏执行 workload，或只执行关联 Service。审计中的纯函数探针已确认 PVC 冲突返回 error，而 `GenerateWebService` 返回 nil；最终工作流状态影响来自调用链检查，未做真实集群复现。

**简化方向。** 让现有生成器和组件 builder 返回 `(result, error)`，沿现有失败路径传播；可以复用 `failedWorkflowGenerationExecution`，不另建错误框架。属性解析也应遵循同一规则：[`pkg/apiserver/event/workflow/job_builder_inputs.go:L179–L192`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job_builder_inputs.go#L179-L192) 与 [`pkg/apiserver/event/workflow/job/job.go:L1221–L1233`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job/job.go#L1221-L1233) 的重复解析器不应把解码失败变成空 Properties。

**后续验证。** 覆盖附加对象冲突、损坏的持久化 Properties、批处理渲染失败，断言 workflow 失败并保留原因；合法空步骤仍按原契约处理。

## A02：版本更新的重复提交路径具有不同原子性

**代码证据。** [`pkg/apiserver/domain/service/application/application_update_version_phases.go:L294–L361`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version_phases.go#L294-L361) 为自动执行、no-op callback 和直接更新选择不同提交实现。`autoExec=false` 的直接路径逐个 `updateComponent` / `addComponent`，最后更新 App 版本；组件更新通过 [`pkg/apiserver/domain/service/application/application_update_version.go:L156–L165`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version.go#L156-L165) 的仓储直接写入。相对地，自动执行路径在 [`pkg/apiserver/domain/service/application/application_update_version_apply.go:L50–L79`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version_apply.go#L50-L79) 使用事务，且 [`pkg/apiserver/domain/service/application/application_update_version_apply.go:L227–L269`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version_apply.go#L227-L269) 已集中处理组件、App 与 workflow 更新。

**触发与代价。** 同一请求修改组件 A、B，A 已写入，B 或 App 写入失败时，直接路径返回错误但 A 的新配置仍保留。新增/删除组件后，同步 workflow steps 的错误在直接路径只记录 warning，仍返回成功。外层分布式锁解决并发互斥，不能回滚此前数据库写入。这里依据的是实际写入路径，尚未对 MySQL 注入中途写失败。

**简化方向。** 收敛共同数据库提交步骤，优先让纯 update/add 的直接路径复用已有事务能力。`remove` 还可能同步删除 Kubernetes 资源，必须单独定义其失败与补偿边界，不能声称数据库回滚可撤销外部副作用，也不能借重构改变 `autoExec=false` 的执行语义。

**后续验证。** 注入第 2 个组件、App、workflow 写入失败，核对原子性；覆盖成功路径及“不创建执行 workflow”的契约。事务保证需在隔离 MySQL 环境验证。

## A03：DBError 包装后丢失标准错误链

**代码证据。** [`pkg/apiserver/infrastructure/datastore/datastore.go:L53–L65`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/datastore/datastore.go#L53-L65) 保存底层 error，但仅实现 `Error()`，没有 `Unwrap()`。[`pkg/apiserver/infrastructure/datastore/sql/driver.go:L108–L120`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/datastore/sql/driver.go#L108-L120) 将查询错误包装为该类型，而 [`pkg/apiserver/interfaces/grpc/server.go:L380–L400`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/grpc/server.go#L380-L400) 依赖 `errors.Is` 识别取消/超时。

**触发与代价。** `datastore.NewDBError(context.Canceled)` 无法被 `errors.Is(..., context.Canceled)` 识别，外层再使用 `%w` 也无济于事；自定义驱动错误同样失去 `errors.As` 可见性。纯 Go 探针已复现取消和超时两种情况。RPC 错误分类因此可能进入通用服务器错误分支；这不意味着客户端取消一定观察到 Internal，传输层也可能先处理取消。

**简化方向。** 给现有 `DBError` 增加 `Unwrap() error`，保留既有 sentinel 和 `*DBError` 分类；不新增错误体系。

**后续验证。** 表驱动检查取消、超时、驱动错误及多层 `%w` 的 `Is/As`；同时验证 gRPC 分类、既有 not-found 与写入错误行为。

## A04：未命名 init/sidecar 每次渲染生成随机名称

**代码证据。** [`pkg/apiserver/workflow/traits/init.go:L46–L48`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/init.go#L46-L48)、[`pkg/apiserver/workflow/traits/sidecar.go:L50–L52`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/sidecar.go#L50-L52) 在 name 为空时调用 `RandStringBytes(4)`。[`pkg/apiserver/domain/service/validation/validation_traits.go:L459–L498`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/validation/validation_traits.go#L459-L498)、[`pkg/apiserver/domain/service/validation/validation_traits.go:L542–L567`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/validation/validation_traits.go#L542-L567) 的校验未要求提供 name。Deployment 更新判定会比较 PodSpec（[`pkg/apiserver/event/workflow/job/job_deploy.go:L532–L550`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/event/workflow/job/job_deploy.go#L532-L550)）。

**触发与代价。** 相同组件声明被重复执行时，未命名容器产生新的名称，Pod template 随之变化，可能触发不必要的 rollout。纯函数探针已确认同一 unnamed-sidecar 输入连续生成的 PodSpec 不相等；未测量真实集群重启次数。这里不需要生成新的随机业务身份。

**简化方向。** 基于组件身份与容器序号确定名称，并校验碰撞。现有模板路径已经使用组件名和序号（[`pkg/apiserver/domain/service/application/application_template_clone_traits.go:L180–L192`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_template_clone_traits.go#L180-L192)），可对齐现有规则；不要增加一套持久化名称实体。应评估旧随机名称转为确定名称时的一次性 rollout。

**后续验证。** 同一输入重复渲染相等、多容器不重名、显式名称保持、重放不会仅因自动名称改变而更新工作负载。

## A05：缓存接口不能传递调用者的取消和截止时间

**代码证据。** [`pkg/apiserver/infrastructure/cache/icache.go:L10–L16`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/cache/icache.go#L10-L16) 的缓存方法没有 `context.Context` 参数；[`pkg/apiserver/infrastructure/cache/redis_cache.go:L59–L78`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/cache/redis_cache.go#L59-L78)、[`pkg/apiserver/infrastructure/cache/redis_cache.go:L123–L132`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/cache/redis_cache.go#L123-L132) 每次从 `context.Background()` 派生独立超时。实际请求路径 [`pkg/apiserver/domain/service/application/application_query.go:L580–L610`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_query.go#L580-L610) 经 [`pkg/apiserver/domain/service/application/application_cache.go:L31–L49`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_cache.go#L31-L49) 读取缓存，已有请求 context 在 helper 处丢失。

**触发与代价。** 慢 Redis 下，调用者取消或 deadline 到达不能取消缓存操作，处理仍受独立超时与 Redis 客户端行为控制。接口本身使正确透传不可能；这不是建议取消现有缓存 miss/失败后的回源策略。

**简化方向。** 给现有缓存方法传入 context，在父 context 上追加必要的操作上限；同步修改已有 helper 即可，不需要后台 worker 或新的重试机制。

**后续验证。** 阻塞读写时取消父 context，验证及时返回及错误 identity；覆盖 miss、损坏缓存删除、写入和失效，保持现有降级契约。

## A06：操作任务存在两套 writer，子记录失败语义不一致

**代码证据。** [`pkg/apiserver/domain/service/application/application_operation_task.go:L64–L161`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_operation_task.go#L64-L161) 的两个 writer 重复构造 `WorkflowQueue` 和 `JobInfo`。`recordAppOperationTask` 在子 JobInfo 写失败时记录日志后继续返回成功；`recordAppOperationTaskInStore` 则返回错误。前者用于 restart 等路径（[`pkg/apiserver/domain/service/application/application_workload_restart.go:L75–L88`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_workload_restart.go#L75-L88)），后者用于事务内 no-op callback task（[`pkg/apiserver/domain/service/application/application_update_version_noop.go:L31–L37`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version_noop.go#L31-L37)）。版本更新收尾还直接丢弃记录函数的错误（[`pkg/apiserver/domain/service/application/application_update_version_phases.go:L391–L404`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version_phases.go#L391-L404)）。

**代价。** 子记录写入中途失败时，部分路径得到 task ID 或触发完成回调，但操作明细并不完整。重复实现让字段维护和完整性保证随入口分叉。操作本身可能已在 Kubernetes 生效，因此不能简单把所有记录失败改成“操作失败”；这是需要明确的契约，而不是 writer 内隐含的策略。

**简化方向。** 保留一个接收 datastore 的写入函数，统一构造与错误传播；调用点决定事务以及“操作已生效但记录失败”的响应/重试语义。确需 best effort 的记录应显式声明。

**后续验证。** task 写入失败、第 N 个 JobInfo 失败、有无 callback、restart/cleanup/no-op 成功路径；将行为策略变更与纯去重分别验证。

## A07：全局 API registry 保存属于 server 的可变 handler 实例

**代码证据。** [`pkg/apiserver/interfaces/api/interfaces.go:L23–L85`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/api/interfaces.go#L23-L85) 用全局 slice、类型集合、互斥锁和 `sync.Once` 管理固定内置 handler。`GetRegisteredAPI` 只复制 slice，没有复制 handler。[`pkg/apiserver/interfaces/api/applications.go:L17–L23`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/api/applications.go#L17-L23) 的 handler 持有业务依赖；[`pkg/apiserver/server_assembly.go:L219–L243`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/server_assembly.go#L219-L243) 将其加入每个 server 的注入容器，[`pkg/apiserver/server_bootstrap.go:L96–L108`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/server_bootstrap.go#L96-L108) 又从全局表绑定路由。

**代价。** 同进程多 server 或测试重复装配时共享可变实例，生命周期不属于实际 owner。registry 锁只保护集合，不能隔离实例依赖。是否二次覆盖或保留首次依赖取决于注入器规则；本项不声称已在线上复现 race。当前生产注册点是固定内置清单，未找到必须动态注册 handler 的需求。

**简化方向。** 每个 server 创建并持有自己的固定 handler slice，同一份 slice 同时用于注入与路由注册。复用已有 `NewXxx`，不再引入 factory registry。

**后续验证。** 两套不同 fake service 的 server 各自路由只命中自身依赖；如允许并发初始化，运行 race 测试。现有 registry 幂等/reset 测试不能证明实例隔离。

## A08：固定 Trait 集合通过字符串、反射与类型断言调度

**代码证据。** [`pkg/apiserver/workflow/traits/processor.go:L116–L132`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/processor.go#L116-L132) 已列明内置处理顺序，但 [`pkg/apiserver/workflow/traits/processor.go:L187–L249`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/processor.go#L187-L249) 仍通过反射取得字段并转为 `interface{}`，[`pkg/apiserver/workflow/traits/processor.go:L375–L389`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/processor.go#L375-L389) 按处理器名称匹配 Go 字段或 JSON tag，[`pkg/apiserver/workflow/traits/resources.go:L23–L26`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/workflow/traits/resources.go#L23-L26) 再断言回具体规格类型。Deployment、StatefulSet、Job 和嵌套容器都走这条路径。

**代价。** 新增/改名需要同时维护规格字段、tag、字符串名称、注册顺序和断言类型；字段未匹配会直接跳过，编译器无法检查对应关系。支持范围仍由固定 `spec.Traits` 决定，动态机制并未免除规格修改。

**简化方向。** 保留有价值的 `TraitResult` 纯结果与聚合边界，将分发改为对 `spec.Traits` 的显式有序访问，复用处理逻辑。不要以简化为名取消冲突检查、顺序约束或嵌套排除，也无需再造插件系统。

**后续验证。** 覆盖每个内置 Trait、空值、处理顺序、嵌套排除和附加对象冲突；确保新增字段有明确处理或明确排除。

## A09：领域规格转换绕经完整展示 DTO，并反向依赖接口层

**代码证据。** [`pkg/apiserver/domain/service/application/component_convert.go:L3–L25`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/component_convert.go#L3-L25) 和 [`pkg/apiserver/domain/service/resourceimport/namespace_import.go:L869–L885`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/resourceimport/namespace_import.go#L869-L885) 先调用 `ConvertComponentModelToDTO`，再拷贝 7 个字段构造 `CreateComponentRequest`。展示转换会生成 ExternalLinks、Services、Ingresses、ResourceConfigs、Credentials 等随后被丢弃的字段（[`pkg/apiserver/interfaces/api/assembler/v1/component.go:L36–L62`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/api/assembler/v1/component.go#L36-L62)、[`pkg/apiserver/interfaces/api/assembler/v1/component.go:L88–L119`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/api/assembler/v1/component.go#L88-L119)）。更新和资源名校验实际使用这条路径（[`pkg/apiserver/domain/service/application/application_update_version.go:L184–L189`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_update_version.go#L184-L189)、[`pkg/apiserver/domain/service/application/application_resource_validation.go:L78–L100`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application_resource_validation.go#L78-L100)）。

**代价。** 领域校验/更新依赖 HTTP 展示模型及其额外计算，展示字段演进会影响非展示流程；application 与 resourceimport 还维护两份相同转换。当前 assembler 已有直接 model→CreateRequest helper，说明完整展示中转并非必要，但仅改调用这个 helper 仍没有解决依赖方向。

**简化方向。** 将“model 解码为业务输入规格”放回现有 domain 归属，接口层负责展示扩展；合并两个领域调用点的重复转换。以这一条组件链路证明收益，不先建立通用 use-case framework。

**后续验证。** version update、资源命名校验、导入回归；保持 nil/empty、Properties、Traits、Secret 和 sidecar 的现有字段语义。

## A10：JSONStruct 的结构体转换绕行 YAML

**代码证据。** [`pkg/apiserver/domain/model/model.go:L50–L62`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/model/model.go#L50-L62) 对已知 Go 值先 `yaml.Marshal`，再 `yaml.Unmarshal` 到 map；目标类型本身是 JSON map。应用创建对每个组件 Properties 和 Traits 均调用此函数（[`pkg/apiserver/domain/service/application/application.go:L918–L930`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/application/application.go#L918-L930)），更新、workflow steps 与导入快照也有消费者。

**代价。** 此处没有外部 YAML 输入，却多维护一层格式往返、分配和错误分支。可以确认冗余编码路径，未做基准测试，因此不称其为已测得的性能瓶颈。

**简化方向。** 在既有 JSON 契约下使用 `encoding/json` 完成结构体到 map 的转换，保留错误返回。替换前比较数字、tag、omitempty 和自定义编码语义，不把“少一次转换”当成行为等价的证明。

**后续验证。** nil/typed nil、空 map、数字、嵌套 Traits、不可编码值，以及创建/更新/回读等价性。旧 Q-021 已修复序列化吞错，本项不重新指控该已修问题。

## A11：Jobs 的必需存储能力仍靠“可选接口”运行时断言

**代码证据。** [`pkg/apiserver/infrastructure/datastore/datastore.go:L199–L230`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/datastore/datastore.go#L199-L230) 将事务、条件 CAS 等作为扩展；[`pkg/apiserver/jobs/artifacts/store.go:L33–L49`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/jobs/artifacts/store.go#L33-L49) 构造时实际要求多项能力，却仍保存成基础 `DataStore`。[`pkg/apiserver/jobs/artifacts/store.go:L96–L107`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/jobs/artifacts/store.go#L96-L107)、[`pkg/apiserver/jobs/service.go:L145–L153`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/jobs/service.go#L145-L153)、[`pkg/apiserver/jobs/sandbox_maintenance.go:L92–L105`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/jobs/sandbox_maintenance.go#L92-L105) 在业务内部反复断言。事务回调参数 `func(tx DataStore)` 又擦除了后续必需能力。

**代价。** 依赖保证分散到构造和执行深处，包装器/测试替身可能满足表面接口却在运行时缺能力；有的调用检查 `ok`，有的直接断言。当前 SQL Driver 已实现相关能力，构造也有部分校验，不应把本项写成正常 MySQL 配置必然 panic。

**简化方向。** 在实际消费边界组合已有能力接口，明确事务对象需要保证的能力，并删除对应的重复断言。避免扩大所有消费者的接口，或给每条业务增加一层仓储包装。

**后续验证。** 缺能力实现尽量在编译/构造阶段被拒绝；保留事务回滚、CAS、数据库时钟及 Sandbox/Checkpoint 所需行锁语义。数据库保证需真实 MySQL 集成验证。

## A12：execution 解析在多个 helper 中重复全量查询

**代码证据。** [`pkg/apiserver/jobs/service.go:L276–L345`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/jobs/service.go#L276-L345) 的 `evaluationExecution` 无分页读取 task 全部 JobInfo，再在 Go 中按 type/key 过滤。显式 key 的 `Get` 先经 `Task` 查询一次，随后再查一次；`ResolveExecutionKey` 也重复这一过程。HTTP 下载在 resolve 后又调用 `Task`（[`pkg/apiserver/interfaces/api/jobs.go:L225–L230`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/api/jobs.go#L225-L230)），gRPC 亦然（[`pkg/apiserver/interfaces/grpc/jobs.go:L381–L387`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/interfaces/grpc/jobs.go#L381-L387)）。因此这些显式 key 的结果下载准备路径包含 3 次相同 task executions 的全量查询。

**代价。** 分层 helper 隐藏了重复 I/O，加载成本随 task 内 Job 数量增加；为了选一条 execution 反复取回全部记录。这里统计的是调用次数，没有推断具体吞吐下降比例。

**简化方向。** 一次解析返回已授权 task 和选中 execution，传给后续流程。有 key 时精确过滤，无 key 时有界读取以判断歧义；需保留现有 executionKey 为空记录的处理语义。现有 [`pkg/apiserver/domain/repository/job_scheduler.go:L690–L712`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/repository/job_scheduler.go#L690-L712) 已展示过滤和有界查询模式，无需新增查询框架。

**后续验证。** recording datastore 检查只解析一次；覆盖无 key 的 0/1/多 execution、错误 workspace/key、command Job、应用内 evaluation 与恢复执行，不能因合并查询跳过授权检查。

## A13：ICache 同时充当 Redis 依赖入口

**代码证据。** [`pkg/apiserver/infrastructure/cache/icache.go:L18–L22`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/cache/icache.go#L18-L22) 暴露 `GetRedisClient`；[`pkg/apiserver/infrastructure/cache/mem_cache.go:L117–L141`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/infrastructure/cache/mem_cache.go#L117-L141) 让内存缓存也携带 Redis 依赖。[`pkg/apiserver/domain/service/internal/schedulelock/app_schedule_lock.go:L24–L44`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/internal/schedulelock/app_schedule_lock.go#L24-L44)、[`pkg/apiserver/domain/service/internal/cancelsignal/cancel_signal.go:L14–L25`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/internal/cancelsignal/cancel_signal.go#L14-L25)、[`pkg/apiserver/domain/service/resourceimport/management_lock.go:L25–L40`](https://github.com/PixelCores/Eruun/blob/d075a82db6b8a9f1dada57a86920321ed8fd4147/pkg/apiserver/domain/service/resourceimport/management_lock.go#L25-L40) 通过缓存对象获取分布式锁/取消通道的客户端。

**代价。** 缓存替换、禁用和测试必须理解无关的协调依赖，缓存可用性与分布式互斥/取消可用性混在同一对象中。A05 是 I/O 生命周期问题，本项是依赖方向问题，两者可分开修复。

**简化方向。** server 装配显式提供已有 `locker.Locker` 或取消通道所需依赖，复用 `ScheduleLocker`、`ManagementLocker` 等现有入口；缓存只承接缓存操作，不再增加服务定位器。

**后续验证。** 替换或禁用读缓存不影响锁与取消；协调依赖缺失仍明确失败；覆盖应用调度、workflow cancel 和资源导入互斥。

## 没有据此列为问题的设计

- 分布式 lease、generation/token fencing、事务、幂等及权限隔离有当前需求依据；不能因为复杂就删掉。
- gRPC 的 ProtoJSON 桥接有运行时转换成本，但复用了自定义 JSON 解码、可选字段 presence 和现有 HTTP/gRPC parity 测试。缺乏具体字段错误或性能证据时，不把全面手写 mapping 当成当然更优的方案。
- 文件较长、接口暂时只有一个实现、纯 `TraitResult` 聚合边界，本身都不足以证明错误抽象。
- 未把“慢下载客户端直接持有 artifact 数据库事务”列为问题：当前 HTTP/gRPC 路径已先写临时文件，不能忽略这层实际行为。
- CloudJob NAS adapter 的 context 传播值得后续专项检查，但当前公共空间策略拒绝 CloudJob，且 SDK 的取消/默认超时语义未完成核验，本次不将它列为公共可达故障。

## 与历史审计的关系和建议顺序

[八月审计](go-idiomatic-code-quality-audit-2026-08-09.md) 和 [原行动映射](code-quality-audit-action-map.md) 是背景，旧编号不在本文重复分配：A09 具体复核 Q-016 的剩余反向依赖；A11 对应 Q-017/Q-018 的剩余能力契约；A07/A08 对应 Q-019 的 registry/类型关系；A05 对应 Q-020 的 context 边界。A10 区分于已关闭 Q-021 的吞错修复。旧文档对文件拆分、JSON 错误传播等已关闭项的结论，不因本次发现而整体重开。本文也没有把原行动映射更新为已完成状态。

建议以小 PR 推进：

1. 先分别修复 A03、A01、A04，以明确错误链、生成失败和确定性输出的契约。
2. A02 与 A06 先补中途失败证据，再统一数据库写入语义；不要混入 Kubernetes 删除策略变化。
3. A05、A12 分别收敛 context 和查询路径，保留授权及现有降级行为。
4. A07–A11、A13 随所属模块逐项简化；用实例隔离、字段等价和构造约束证明收益，不启动全仓框架替换。

## 本次验证与限制

- `go version`：`go1.27.1 darwin/arm64`。
- `go vet ./...`：通过。
- `go test ./pkg/apiserver/domain/model ./pkg/apiserver/infrastructure/datastore/... ./pkg/apiserver/infrastructure/cache ./pkg/apiserver/workflow/traits ./pkg/apiserver/interfaces/grpc -count=1`：通过；包中真实 MySQL 测试要求 `MYSQL_TEST_DSN`，本次未提供隔离 MySQL 环境，不作为事务/数据库时钟集成验证。
- 临时、未入库的纯 Go 探针：确认 `DBError` 的取消/超时错误 identity 丢失；确认相同 unnamed-sidecar 输入输出不稳定；确认冲突 Trait 的错误被资源生成器转为 nil。这些探针没有访问 Kubernetes 或云资源。
- 本 PR 仅新增本审计文档并更新文档索引；未修改运行代码、未声称修复上述问题。没有运行全仓 race/coverage、真实集群或云端故障注入；这些属于对应后续修复的验证要求。
- 本机没有 `staticcheck` / `gocyclo` 可执行文件，本次未安装或据此生成分数；不以函数长度或复杂度阈值替代调用链证据。
