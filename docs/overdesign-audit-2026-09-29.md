# 全仓过度设计审计（2026-09-29）

> 状态：Historical / Audit。审计基线为 `main@b7268a82c9fca3bc928a607c83fa196a37f58b5a`，问题证据链接固定到该提交。后续简化已在 [PR #117](https://github.com/PixelCores/Eruun/pull/117) 分支实施，处置与验收见下文；不据此声明已合入 `main` 或完成线上验收。

## 结论与范围

审计时最值得处理的是 **Try 与写入路径重复维护业务校验规则**，以及 **数据库配置暴露了不参与连接的第二份库名**。另外确认了 CloudJob 内部参数副本、运行快照搬运和无生产消费者的错误处理工具。这些问题都有具体代码证据，但未据此证明线上事故或性能瓶颈；本轮按 **P3 维护债务与配置表达问题**记录，推进顺序不等同于故障严重程度。

首轮基于 `9097b26` 的 O01–O07 已有对应简化进入本轮基线。本文复用同一审计文件，保留原编号的处置表，新发现从 O08 开始；首轮原文可查看[固定历史版本](https://github.com/PixelCores/Eruun/blob/ae9306fc816e9e9845a16dd7bcefd91542bb2e6d/docs/overdesign-audit-2026-09-29.md)，不把已修复项再次列为待办。

审查从 `docs/README.md` 路由到启动装配、HTTP/gRPC、领域校验与服务、Workflow/Job/Traits、Harbor、基础设施、配置和部署，沿实际装配及调用链取证。不是逐行穷尽审计；生成的 Protobuf 代码、第三方依赖体积、文件长度和接口数量不单独作为问题依据。Draft / Proposal 只用于理解方向，不作为删除或保留实现的充分理由。

| 编号 | 基线中的问题 | 代价 | 建议顺序 / 改动边界 |
| --- | --- | --- | --- |
| O08 | Try 与写入重复实现 Trait 叶子校验 | 同一规则需要维护两份实现 | 优先；复用已存在的共享函数，保留入口编排 |
| O09 | 数据库名有无效配置入口，后端帮助列出未支持值 | 增加配置歧义与排障成本 | 优先；独立处理公开配置清理与迁移说明 |
| O10 | CloudJob 检查点写入未被恢复逻辑读取的顶层参数 | 多一份拷贝、序列化和数据来源 | 其次；只收敛内部记录，保留旧记录读取 |
| O11 | CloudJob 初始化后把运行快照搬入无后续内置消费者的 context | request、context、runtime 三处传递增加理解成本 | 其次；先锁定内置 Provider 与扩展契约边界 |
| O12 | `errhandler` 整包只由自身测试使用 | 保留无当前业务需求的策略及导出 API | 低成本清理；核对仓外 Go 消费后移除 |

## 后续实施与验收边界

O08–O12 和下列四个局部候选已在本 PR 分支完成。后文保留审计时的需求、证据和建议，供核对改动原因，不再作为当前分支的未完成清单。

| 项目 | 已实施的收敛 | 保留的行为与验证 |
| --- | --- | --- |
| O08 | Try 复用已有 `internal/traitvalidation` 的 Service、Ingress、Rollout 规则，移除规则副本 | Try 的保留标签错误仍排序并指向具体 key；保留 Service 名称先于标签的错误顺序、字段/code/message、完整报告及未改写的 `normalizedSpec`。新增报告回归先在旧实现通过，再验证新实现；现有写入、nested、nil/零值测试保留 |
| O09 | 移除 `Datastore.Database`、`--datastore-database` 和默认配置副本；DSN 是唯一选库来源。清除 `DTMAddr`、`IstioEnable`、`AddonCacheTime` 无消费者预留；后端帮助和校验均只接受 `mysql` | 旧环境变量明确拒绝，含空值；flag/env 优先级和 DSN 库名解析有回归。保留连接池及三种 schema 模式；未知后端在启动校验阶段失败 |
| O10 | 新检查点不再写入顶层 `CloudJobRecord.Params` | 旧 JSON 中该字段可被忽略并正常解码；恢复仍使用 `Request.Params`，没有 request 的旧记录仍走原有回退。两种 `ExecutionKey`、state 和错误保留 |
| O11 | 删除初始化后的 snapshot context 搬运及专用导出 helper；runtime 持有已初始化依赖，request 的瞬态 snapshot 只在内存传递 | 两轮 action 推进检查中间及最终持久化记录，snapshot/模拟凭据均不落库；运行期设置变更不替换已有 runtime；缺少可信 snapshot 的恢复仍拒绝 |
| O12 | 删除仅由自身测试使用的 `utils/errhandler` 包及对应测试 | 仓内导入图无生产消费者；既有启动、Worker 和 Leader 错误路径未改动 |
| Aliyun action | 三个空结构体 action 直接保存在 Provider 中，删除空构造函数 | 每个 action 以 8 个并发独立输入/state 验证隔离；保留自定义 Provider 工厂协议 |
| Workflow 入队 | 多层默认参数转发收敛为 `CreateWorkflowQueueTask` 和 `QueueTaskOptions`，三个实际调用点显式传入所需值 | 保留调用方事务与调度锁、重复键查询/错误、callback/cleanup/resourceAction 快照及调度时间；现有入队和版本更新测试覆盖 |
| 单用途 DTO 转换 | 工作流列表直接转换，移除只有一个消费者的泛型 helper | 保留 nil 元素过滤、空数组响应、首个转换错误返回及日志 |
| 聚合状态依赖 | `applicationstatus.Service` 使用只声明实际查询方法的 `WorkflowTaskReader` | HTTP/gRPC 装配不变，状态与 active update 判断由现有接口回归覆盖 |

**升级与源码接入。** 移除 `--datastore-database` 和所有环境中的 `ERUUN_DATASTORE_DATABASE`（包括空值），核对 `--datastore-url` / `ERUUN_DATASTORE_URL` 的 DSN 仍指向原数据库；`MYSQL_DATABASE` 的 MySQL 初始化及 DSN 生成语义不变。操作说明见 [本地依赖](local-docker-dependencies.md) 和 [Helm 部署](helm-deployment.md)。没有新增第二个选库机制。

仓外 Go 消费者未验证。删除的导出源码入口包括 `CloudJobRecord.Params`、snapshot context helper、旧 `CreateWorkflowQueueTaskWith…` 函数族和 `errhandler`；配置结构的无效字段也已移除。源码集成应改为 `CreateWorkflowQueueTask(..., QueueTaskOptions{...})`，自定义 CloudJob 按 [Provider 模板](cloudjob-custom-provider-template.md) 使用 runtime 或 request 的瞬态字段。公共 HTTP/gRPC 路由与 JSON 响应未改变；内部新检查点省略不参与恢复的顶层 `params`，没有修改数据库表结构。

## O08｜同一 Trait 校验规则在 Try 与写入中各实现一遍

**当前需求与证据。** Try 应返回完整校验报告，写入应在无效请求落库前失败；两条入口需要不同的错误编排，但底层规则相同。目前 Try 调用 `validation` 包内的副本（[调用点](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/validation/validation_traits.go#L51-L81)），Create/Version 写入调用已存在的 `internal/traitvalidation`（[写入调用点](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/application/application.go#L965-L992)）。

| 规则 | Try 实现 | 写入共享实现 |
| --- | --- | --- |
| Service 类型、名称、selector、port/protocol | [validation_service.go:16–119](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/validation/validation_service.go#L16-L119) | [trait_validation.go:255–358](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/internal/traitvalidation/trait_validation.go#L255-L358) |
| Ingress 名称、host、backend 引用 | [validation_ingress.go:18–112](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/validation/validation_ingress.go#L18-L112) | [trait_validation.go:159–252](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/internal/traitvalidation/trait_validation.go#L159-L252) |
| Rollout 类型、整数/百分比、零值组合 | [validation_traits.go:132–333](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/validation/validation_traits.go#L132-L333) | [trait_validation.go:360–498](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/internal/traitvalidation/trait_validation.go#L360-L498) |

**为什么值得简化。** 叶子规则在两套实现中重复，Rollout 也重复对应规则；实施复核确认保留标签的错误字段、消息和顺序存在入口差异，因此仅复用规则，保留 Try 的小型呈现适配。这里的问题不是文件太长，而是共享校验包已经存在，另一入口仍维护规则副本。一次规则调整必须定位两套函数，遗漏其中一处就可能使 Try 与提交产生分歧。本轮未发现足够证据宣称两条路径已经发生规则漂移，也不把相关区间总行数当作可直接删除的行数。

**最小简化。** 先让 Try 的 Service、Ingress 叶子校验调用现有共享函数，再按相同边界收敛 Rollout；删除失去消费者的副本和局部 helper。无需新建 validation engine、注册表或 package。这不是重开历史 Q-004 的大文件拆分问题。

**保留与验收。** 保留 Try 收集全部错误、写入返回首个业务错误的差异，以及字段路径、code/message、错误顺序、nested Trait 限制、nil/零值语义。用同一组有效/无效输入覆盖两条入口，断言规则一致、响应形式各自不变；不能通过取消 Try 中的校验来减少代码。

## O09｜数据库身份保留了一个可设置但无运行消费者的副本

**当前需求与证据。** [配置入口](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/config/config.go#L358-L365)同时提供 `--datastore-url` 和 `--datastore-database`；[配置结构](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/infrastructure/datastore/datastore.go#L72-L83)、[默认值](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/config/config.go#L169-L178)及[参考配置](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/config/apiserver-default.yaml#L14-L19)也保留两份库名表达。环境变量映射会接受 `ERUUN_DATASTORE_DATABASE`。但 [openDatabase](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/infrastructure/datastore/mysql/mysql.go#L79-L109)只从 `cfg.URL` 生成连接 DSN，没有读取 `cfg.Database`；仓内未找到该字段的其他运行消费者。

一个只解析 flag 与 DSN、不连接数据库的本地探针使用 `.../from-dsn` 和 `--datastore-database=from-flag`，结果为：

```text
flag_database=from-flag dsn_database=from-dsn
```

这证明参数被接受且两个值并存；结合 `openDatabase` 的取值路径，可以确认后者不会覆盖连接库名，不代表完成了真实数据库连接验证。另外 `datastore-type` 帮助声称支持 `mysql, tidb`，[运行装配](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/server_assembly.go#L80-L93)却只有 `mysql` 分支。

**代价与最小简化。** 配置使用者需要判断两份数据库名哪个有效，维护者需要解释无效开关与不可用后端。建议明确以 DSN 为唯一数据库身份来源，修正后端帮助，独立清理无效字段、flag/env 和参考项，并给已有配置使用者迁移说明及明确的拒绝行为。不要突然让旧字段覆盖 DSN；那会改变实际数据库选择，超出结构简化。也不能从 `tidb` 类型值不可用推导 TiDB 的 MySQL 协议兼容性结论。

**保留与验收。** 保留连接池、凭据占位符拒绝、schema 的 migrate/validate/migrate-only 行为。验证 flag/env、DSN 库名解析、未知后端拒绝及部署清单的 DSN 展开；特别检查旧环境变量不再静默产生“设置成功”的错觉。本轮未连接数据库，未复现误写其他库。

**同类低收益候选。** `DTMAddr`、`IstioEnable`、`AddonCacheTime` 目前只有[字段声明](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/config/config.go#L59-L82)和[默认赋值](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/config/config.go#L192-L195)，没有 flag 或运行消费者。可清理预留字段，不应为了保住它们而补建新的功能。

## O10｜CloudJob 检查点持续写入未被使用的顶层参数

**当前需求与证据。** [CloudJobRecord](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L26-L35)同时保存 `Params` 和 `Request`。每次持久化检查点时，[记录函数](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L188-L198)从 `info.Params` 克隆顶层参数，再克隆含 `Params` 的请求；[恢复函数](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L263-L293)实际读取的是 `checkpoint.Request.Params`。仓内未找到顶层 `CloudJobRecord.Params` 的生产读取。

记录最终写入 `JobInfo.InternalInfo`；[模型字段](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/model/job.go#L20-L25)标记为 `json:"-"`。它是内部检查点，不能说成公共 API 已承诺的字段。另一方面，`info.Params` 与恢复后的 `Request.Params` 可能来自不同时刻，也不能声称两者永远相等。

**代价与最小简化。** 当前恢复只依赖一条参数来源，却每轮额外拷贝、编码另一份未读取参数；没有测得其存储或性能收益。可先停止向新检查点写顶层 `Params`，保留 `Request.Params`、旧记录解码和错误路径，避免一次性重写记录格式。

**保留与验收。** 覆盖参数恢复、旧记录、错误检查点及脱敏。特别不能顺手合并两个 `ExecutionKey`：顶层来自组件的 `CloudJobInfo`，请求中的值来自 `JobTask`，它们承担不同身份语义（[赋值](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L191-L198)、[请求身份](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L263-L272)）。

## O11｜运行快照在初始化之后被搬进没有后续内置读取的 context

**当前需求与证据。** CloudJob 需要在一次执行期间使用稳定的云配置，并禁止把凭据/运行快照写入检查点。当前 [Run](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L111-L125)先调用 `provider.NewRuntime`，随后才调用 [attachCloudJobRuntimeProviderSnapshot](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/job_cloud.go#L394-L405)，把 request 中的快照转入 context 再清空 request 字段。

当前唯一内置 Provider 的 context 快照读取在 [Aliyun.NewRuntime](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L51-L61)，发生在上述搬运之前；仓内非测试调用中没有搬运后的第二次 `NewRuntime`。三个内置 action 也不读取此 context 值，而实际配置已经保留在[返回的 runtime client](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L84-L96)。为这条当前未被消费的后置通道，仍需维护 [context key、provider 名归一化、存取和类型断言](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/cloudjob/contracts/context.go#L35-L60)。

**代价与最小简化。** 读者需要在 request、context、runtime 之间追踪同一轮配置，才能确定它是否参与恢复。对当前内置执行，配置稳定性可以由已构造的 runtime 承担；建议收敛后置搬运，不新增通用上下文容器。此结论限于本仓当前装配，删除导出 helper 或改变自定义 action 可见的 context 前，仍需核对仓外 Provider；现有测试中的假 action 读取快照，不等于存在内置业务消费者。

**保留与验收。** 保留运行快照与凭据不落库、一次执行配置稳定，以及已有持久化 state 却缺少可信运行快照时[明确拒绝恢复](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L55-L61)。不能删掉拒绝检查后改成读取新的系统设置继续执行。验证新执行、多轮推进、初始化失败、跨重启拒绝、checkpoint 内容及扩展 Provider 边界。

## O12｜没有生产消费者的错误通知策略包

**当前证据。** [`utils/errhandler/handlers.go`](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/utils/errhandler/handlers.go#L3-L54)为错误通知维护 `ErrorHandler`、nil channel 策略枚举、options、fallback 回调及三个导出函数。全仓调用/导入搜索只命中该包及自身测试；Go 的 `cmd/...`、`pkg/...` 非测试依赖图中没有包导入它。

**代价与最小简化。** 没有当前业务消费者，却仍维护 panic/ignore/fallback 的组合及其测试。若确认没有仓外 Go 包依赖，可直接删除这个孤立包与仅服务它的测试；无需将其接入现有代码来证明抽象“有用”。54 行生产文件只是维护表面大小，不是性能结论。

**保留与验收。** 不改变现有启动、Worker、Leader 的错误传播。删除前核查导入图和外部消费，删除后编译实际调用包。不能因这个包无消费者就推导所有错误处理 helper 都多余。

## 候选与不建议实施的扩大化重构

- **Aliyun 内部 action 工厂。** [Provider 的 action 工厂映射](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/cloudjob/aliyun/provider.go#L19-L44)把三个空结构体 action 包在工厂中；例如 [NAS action 构造](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/cloudjob/aliyun/action_nas_ensure_filesystem.go#L11-L15)只返回空实例。可局部考虑直接保存无状态 `CloudAction`，但收益较小，须验证并发无共享可变状态。保留 provider/action 协议、白名单、状态机和[自定义 Provider 扩展模板](cloudjob-custom-provider-template.md)，不据此删除整个扩展层。
- **单用途转换与默认参数转发。** [`convertDTOList`](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/interfaces/api/application_workflow.go#L28-L57)只有一个生产消费者；[Workflow 入队函数族](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/workflow/workflow.go#L2163-L2215)通过多层默认参数包装同一操作。可随邻近改动收敛，不值得新建通用框架；入队事务、重复键幂等及 callback/cleanup/resourceAction 快照必须保留。
- **窄消费者仍依赖整块服务。** [`applicationstatus.Service`](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/service/applicationstatus/status.go#L24-L34)只需要 `HasImmediateActiveVersionUpdateTask`，仍声明 `ApplicationsService`。可以沿这一实际消费者收窄，但旧 O04 的 32 个空方法证据已经失效，不能把局部剩余问题描述为原修复未完成。
- **保留必要复杂性。** Workflow 租约、generation/token fencing、outbox、数据库恢复、持久卷身份、空间授权和 Harbor 的结果确认/恢复屏障均有当前需求。HTTP/gRPC 并行与 ProtoJSON 数字处理有公开协议约束。没有测量与替代方案证据，不能把这些机制或整套 IoC 一次性删除。

## 首轮 O01–O07 的当前处置

以下状态只表示原报告的具体证据已失效或已处置，不表示相关模块不存在其他维护债务。合并记录可由固定基线的 `git log 9097b26..b7268a8` 复核。

| 旧项 | 合并记录 | 本轮基线核对 |
| --- | --- | --- |
| O01 缓存不可达选项 | [#97](https://github.com/PixelCores/Eruun/pull/97) | Redis-only 帮助与运行校验一致；[直接构造 Redis 缓存](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/server_assembly.go#L125-L129)，缺少客户端明确失败 |
| O02 非 API 角色装配业务适配器 | [#95](https://github.com/PixelCores/Eruun/pull/95) | [New 按角色选择 handler](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/server.go#L105-L116)，[gRPC 业务适配器仅在 API 角色装配](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/server_assembly.go#L255-L270) |
| O03 延迟通知重复完整 Job | [#101](https://github.com/PixelCores/Eruun/pull/101) | [v2 通知只传身份及调度信息](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/workflow/job/delay_queue.go#L36-L75)，完整 workload 仍由数据库保存 |
| O04 Workflow 局部消费者依赖整块应用服务 | [#98](https://github.com/PixelCores/Eruun/pull/98) | [独立 Workflow handler](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/interfaces/api/application_workflow_routes.go#L10-L24)只注入 WorkflowService；原空方法替身已收敛 |
| O05 锁后端工厂与占位 | [#96](https://github.com/PixelCores/Eruun/pull/96) | [直接构造 Redis locker](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/server_assembly.go#L133-L143)；旧 Type/Config 工厂及 Metadata 预留已移除 |
| O06 Worker 冗余动态断言 | [#99](https://github.com/PixelCores/Eruun/pull/99) | [Worker 直接表达订阅能力](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/event/event.go#L19-L28)，启动不再断言回同一接口 |
| O07 JSONStruct 无消费者的旧转换入口 | [#100](https://github.com/PixelCores/Eruun/pull/100) | [model.go](https://github.com/PixelCores/Eruun/blob/b7268a82c9fca3bc928a607c83fa196a37f58b5a/pkg/apiserver/domain/model/model.go#L3-L46)已移除 RawExtension/string 入口和 YAML 转换依赖 |

本轮也核对了后续生成对象、Job runtime 注入、缓存接口和角色观察器的简化；没有沿用这些代码变动之前的结论。其他历史质量报告及行动映射只作为线索，不作为当前未修问题清单。

## 验证记录

### 审计基线

审计初稿提交 `3965b4f` 仅修改本文与文档索引。以下命令在当时基线通过，用于确认原有行为，不能单独作为后续重构的验收结果；其中 `TestCloudJobCtlRunKeepsRuntimeProviderSnapshotInContextOnly` 已在实施中改名为 `TestCloudJobCtlRunKeepsRuntimeProviderSnapshotOutOfCheckpoints`：

```sh
# 角色装配、观察器所有权与 Worker readiness
go test ./pkg/apiserver -run 'TestRuntimeRolesOnlyBuildTheirServedAPIAdapters|TestStartWorkersSkipsNilWorkerInReadinessCount|TestBuildRuntimeQueuesBuildsOnlyRoleQueues|TestInitRoleObserversBuildsOnlyOwnedObserver' -count=1

# Redis、锁与 JSONStruct 的已修复旧项
go test ./pkg/apiserver/config ./pkg/apiserver/infrastructure/cache ./pkg/apiserver/infrastructure/locker ./pkg/apiserver/domain/model -run 'TestCacheTypeFlagRejectsUnsupportedEnvironmentValue|TestNewRedisICacheRequiresClient|TestNewRedisLockerRequiresClient|TestNewJSONStructByStructPreservesJSONContract' -count=1

# 延迟通知及独立 Workflow handler
go test ./pkg/apiserver/event/workflow/job ./pkg/apiserver/interfaces/api -run 'TestDelayedNotificationDispatchesCommittedWorkload|TestLegacyDelayNotificationRejectsChangedWorkload|TestApplicationWorkflowHandlerInjection' -count=1

# Try / 写入校验的现有契约
go test ./pkg/apiserver/domain/service/validation -run 'TestValidationService_TryApplication_(ValidServiceTrait|InvalidServiceTraitMissingPorts|InvalidServiceTraitHeadlessType|InvalidIngressNameAndHosts|RejectsMissingIngressServiceNameWithMultipleServices)|TestValidateComponentTraitsForWriteRejects(InvalidServiceTrait|ReservedIngressLabels|AmbiguousIngressBackend)' -count=1

# CloudJob checkpoint、运行快照和跨重启拒绝边界
go test ./pkg/apiserver/event/workflow/job ./pkg/apiserver/event/workflow/cloudjob/aliyun -run 'TestCloudJobCtlRunWithRegisteredProviderSuccess|TestCloudJobCtlRunKeepsRuntimeProviderSnapshotInContextOnly|TestCloudJobCtlRunFailsToResumePersistedStateWithoutRuntimeProviderSnapshot|TestCloudJobCtlRunMatchesCheckpointByExecutionKey|TestProviderNewRuntimeRejectsResumeWithoutRuntimeProviderSnapshot|TestProviderNewRuntimeUsesRuntimeProviderSnapshotOnResume' -count=1
```

上述测试均通过。另外完成源码调用/导入搜索、flag/DSN 内存解析探针、文档链接及行号边界检查、`git diff --check` 和 `scripts/check-sensitive-content.sh`。

审计初稿未修改 Go 代码或运行配置，未运行全仓 race/coverage。后续实施的验收独立记录如下。

### 后续实施

新增回归覆盖 Try 错误呈现和规范化请求、旧数据库输入拒绝、DSN 唯一选库、旧 CloudJob 检查点恢复、运行快照不落库及共享 Aliyun action 的并发隔离。已完成限定差异及直接调用链的独立复核。

通过的本地检查：

```sh
go test -race -cover -p 2 ./...
go vet ./...
go build -trimpath -o /tmp/eruun-cleanup-server ./cmd/main.go
deploy/all_in_one_install_quickstart_test.sh
# 使用 Helm v4.2.0，HELM_BIN 指向本地可执行文件
HELM_BIN=helm deploy/helm/eruun/helm_template_test.sh
scripts/check-sensitive-content.sh
git diff --check
```

全仓 Go 格式检查通过；同时修正 main 基线上已有的三处 import 排序差异。除格式外，后续复核没有扩展到新的审计主题。生产 Go 文件相对审计初稿净减少 558 行，此数只描述维护代码规模；新增回归测试单独计算。

真实 MySQL/Redis/Kafka/Kubernetes/ACS、云 SDK 和仓外 Go 消费者未验收；没有进行性能基准，删行与去重不代表已测得吞吐或延迟提升。Workflow 租约、fencing、outbox、恢复屏障和空间授权保持原有实现。
