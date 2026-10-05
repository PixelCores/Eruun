# 代码质量与局部复杂性审查（2026-10-05）

> 状态：Historical / Audit。审查基线为远端 `main@2e0d55b6afb9af2d71e68d92d3afd91b330415b9`。本 PR 仅提交审查结论，以下建议均未实施，不代表新的运行时契约。

## 结论与范围

确认 7 项可以在既有模块内部处理的问题：3 项存在具体行为或契约不一致，4 项属于维护债务。两项行为问题已通过临时 Go overlay 测试复现；恢复材料限额差异由当前各端代码和文档直接核对，未进行大包或真实恢复验收。

本轮保持 IOC 容器、注入标签、Bean 装配、API / Domain / Repository / Infrastructure 分层和四角色运行架构。接口只有一个实现、文件较长、层数较多，都不单独构成问题。判断依据是现有需求是否被重复表达、信息是否先丢失再重建，以及是否维护了没有实际消费者的扩展能力。

沿用历史编号：O22–O26 在历史提交 `8c5a98ac3bf838d18493970dac1a5b8aecc9c690` 中有整改记录，但该提交不属于本次 main 的祖先；本轮逐项重新核对 main，未将历史验证当成本次通过证据。O27、O28 为本轮新确认项。O01–O21 的已合入整改不重复列入。

| 编号 | 优先级 | 当前问题 | 最小处理范围 | 状态 |
| --- | --- | --- | --- | --- |
| O27 | P2 | 操作结果先拼展示文本，再反解析，括号错误会破坏记录字段 | 保留现有结构化操作记录，仅在响应边界格式化 | 待审阅，未实施 |
| O22 | P2 | Service 摘要和访问链接分别推导，端口和可见性已不一致 | 在现有 assembler 内由摘要生成链接 | 历史已知，main 仍存在 |
| O24 | P2 | 三类归档挤入 dataset/result 二值分类，checkpoint 限额跨端漂移 | 用现有归档类别表达预算，统一 checkpoint 限制口径 | 历史已知，main 仍存在 |
| O23 | P3 | 同一 checkpoint 为取 manifest 再完整解压一次 | 在首轮验证扫描中保留 manifest | 历史已知，main 仍存在 |
| O25 | P3 | 当前容器的 Trait 结果使用按名称路由的三组 map | 保留 TraitResult 边界，收敛为当前递归的 slice | 历史已知，main 仍存在 |
| O26 | P3 | Secret 字符串索引套多层结构，并维护恒为 true 的 ready | 私有索引回到嵌套字符串 map | 历史已知，main 仍存在 |
| O28 | P3 | Job 运行策略暴露无人替换的 validator 参数 | 删除私有可选参数，保留默认身份校验 | 待审阅，未实施 |

P2 表示当前可达路径上的具体有界问题；P3 表示有实证的维护成本，不等同于运行故障。本报告没有据代码行数估算性能收益或承诺删减比例。

## O27：结构化操作结果经展示字符串往返后失真

**调用与证据。** HTTP / gRPC 生命周期操作进入现有 application service。以重启为例，[reporter.record](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/domain/service/application/application_workload_restart.go#L293-L322) 已同时拥有资源身份和 `error`，却只保存 `"%s (%v)"` 展示字符串。停止/启动及清理 reporter 使用相同表示。[buildRestartJobRecords 等函数](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/domain/service/application/application_operation_task.go#L114-L150) 随后调用 [parseResourceFailure](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/domain/service/application/application_operation_task.go#L193-L224)，以最后一个 `" ("` 拆回资源名和错误，供 [JobInfo 写入](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/domain/service/application/application_operation_task.go#L91-L108) 使用。

**已复现。** 临时测试直接调用现有 `newRestartReporter`、`record` 和 `buildRestartJobRecords`，输入资源 `Deployment:default/api`、错误 `request failed (retry exhausted)`。期望保留两字段，实际待写入记录为：

```text
name   = "Deployment:default/api (request failed"
errMsg = "retry exhausted)"
```

展示文案因此成为内部持久化协议，修改格式或遇到带括号的错误都会影响结构化记录。本次没有连接数据库验证落库；字段失真发生在真实生产函数生成记录时，后续赋值路径已核对。

**最小方向与约束。** 复用现有 `operationJobRecord` 保留 target/error，只在构造 API 响应时生成 `FailedResources` 文本。不要新增通用 reporter 框架或合并 service。保留响应格式、记录顺序、成功/跳过/失败分类、部分成功语义、错误链、事务原子性、回调时序和 mutation/adopted 锁。

**后续验收。** 为嵌套括号、普通错误、成功和跳过结果验证记录字段及响应；用既有 `TestOperationTaskAtomicWrites`、`TestLifecycleRecordFailureKeepsKubernetesEffectsExplicit` 和生命周期回调测试约束持久化与部分成功行为。

## O22：Service 摘要与链接维护两套推导规则

**调用与证据。** 组件列表经 `ConvertComponentModelsToDTO` 组装返回。[基础转换先生成 ExternalLinks，随后 enrich 才生成 Services](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/interfaces/api/assembler/v1/component.go#L68-L99)。[链接逻辑](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/interfaces/api/assembler/v1/component_link.go#L51-L114) 只从 `Properties.Ports` 取端口，并自行选择名称；[摘要逻辑](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/interfaces/api/assembler/v1/component_service.go#L15-L76) 则优先解释 Service trait 的 `Port/TargetPort`。

**已复现。** 临时测试调用真实 `ConvertComponentModelToDTO`：

| 输入 | Services 摘要 | 当前 ExternalLinks | 应有链接 |
| --- | --- | --- | --- |
| Properties 8080；Service port=80、targetPort=8080 | 80 → 8080 | `api-fixed.default.svc:8080` | `api-fixed.default.svc:80` |
| 仅声明上述 Service trait | 80 → 8080 | 空 | `api-fixed.default.svc:80` |

**最小方向与约束。** 在现有 assembler 内先生成 Services，再据摘要生成 Service 链接，删除第二套名称和端口推导。保留 Ingress 优先、首个非 External Service 选择、去重和顺序、共享资源名、默认 namespace、非 Service 组件排除规则。使用 Service Port，不能使用 TargetPort。

**后续验收。** 保留 `TestConvertComponentModelToDTOUsesExplicitServiceTraitNameForSvcLink`、`TestConvertComponentModelToDTOPrefersNonExternalServiceTraitForSvcLink` 等现有用例，并加入上述两种不同端口/trait-only 输入。无需改 DTO 或 IOC。

## O24：二值归档分类不足以表达现有 checkpoint 需求

**证据。** [归档限额及 readArchive 的 dataset bool](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/artifacts/archive.go#L24-L75) 只能选择 dataset 或 result 预算；[checkpoint 传 false](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/artifacts/checkpoint.go#L22-L30) 后沿用 result 的 512 MiB 压缩、2 GiB 展开限制。[HTTP 入口](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/interfaces/api/jobs.go#L362-L369) 同样允许 512 MiB。

Runner 的 [常量](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/runners/harbor/recovery_runtime.py#L26-L29) 和 [下载入口](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/runners/harbor/recovery_runtime.py#L114-L126) 则限制为 64 MiB / 256 MiB；[现有使用文档](harbor-runtime-recovery.md#生命周期与部署) 也说明了这组 Runner 上限。因此存储入口的接收上界不能保证 Runner 可以消费材料。

此外，Go 的展开预算覆盖完整 tar 流；Python [load_bundle](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/runners/harbor/recovery_runtime.py#L194-L209) 累加 `member.size`，[create_bundle](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/runners/harbor/recovery_runtime.py#L303-L326) 累加文件载荷，均不等价于包含 tar/PAX header 和 padding 的完整展开流。这是静态确认的限额差异，未用真实 Runner 或大型材料复现恢复失败。

**最小方向与约束。** 使用已有 `KindDataset/KindSource/KindCheckpoint` 表达三类归档；Go 侧 checkpoint 常量供 HTTP 和 storage 共用，Python 侧明确采用同一完整流预算。无需策略接口或注册表。限制收紧会改变服务端可接受输入范围，应作为契约修正明确记录，不能混称为纯重构。

**后续验收。** 验证 64 MiB 压缩、256 MiB 完整展开流、1 MiB manifest 边界，以及 tar/PAX/padding、gzip 截断、HTTP/storage 一致性和失败无写入。保持身份、摘要、文件清单和恢复事务保护。

## O23：获取一个 manifest 导致同一归档完整扫描两遍

**调用与证据。** Runner checkpoint 上传经 Jobs service 进入 [PutCheckpoint](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/jobs/artifacts/checkpoint.go#L22-L64)。第一轮 `readArchive` 已遍历并哈希所有文件、验证路径和 gzip 完整性；随后又创建 gzip/tar reader，从头扫描到 EOF，只为取出 `checkpoint.json`。第二套读取/关闭/错误分支还没有复用首轮的 `contextReader`。

**维护成本。** 每次 checkpoint 上传多一次完整解压，并重复维护归档读取控制流。这是代码路径上的重复工作，未测量 CPU、吞吐或峰值内存收益。

**最小方向与约束。** 首轮验证扫描顺便保存有上限的 manifest，`PutCheckpoint` 直接消费。归档文件摘要与 checkpoint 声明的逐项对比仍须保留，它们验证不同事实。不要为减少扫描删除 gzip/tar 完整性、路径/重复项检查、工作空间锁、事务内 bind 或同 ID 内容不可变保护，也不引入新归档框架。

**后续验收。** `TestCheckpointMaterialFileManifest`、`TestArchiveRejectsUnsafeAndInvalidInputs`、`TestArchiveBoundsAndCancellation`，并覆盖 manifest 任意位置、无效/超大 manifest、取消、损坏及 bind/保存失败无写入。与 O24 涉及同一入口，可在一个受限实现单元内处理，但分别验收遍历复用和限制契约。

## O25：递归已确定目标容器，却再通过名字路由结果

**证据。** [TraitResult](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/workflow/traits/processor.go#L34-L60) 用三组容器名 map 表达 mounts/env/envFrom。现有 env/storage 生产者写当前组件名；[聚合器](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/workflow/traits/processor.go#L226-L247) 再按 key 合并。[init](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/workflow/traits/init.go#L46-L74) 已取得本次递归结果，却遍历 map 还原 slice；[sidecar](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/workflow/traits/sidecar.go#L50-L67) 则通过外层组件名反查。

Deployment、StatefulSet、即时 Job 和 CronJob 都实际调用 `ApplyTraits`。当前递归结果已经拥有目标容器语境，却还维护生产、聚合、反查和扁平化约定，没有证明这里需要任意容器路由。

**最小方向与约束。** 保持 TraitResult 和 Trait 处理架构，仅将三个字段表示为当前递归的 slice；init/sidecar 消费自己的结果，顶层应用到主容器。保留 mountPath 去重、Trait 顺序、嵌套排除、容器名碰撞拒绝、Volume/对象去重和 StatefulSet PVC/VCT 语义。`TraitResult` 为导出 Go 类型，外部 Go 源码消费者需要单独核查；HTTP/JSON 无需变化。

**后续验收。** `TestApplyTraits_InitTrait_WithNestedTraits`、`TestApplyTraits_SidecarTrait_WithNestedTraits`、`TestApplyTraitsPreservesNestedExclusions`、`TestApplyTraitsPreservesProcessingOrder`、`TestApplyTraitsRejectsContainerNameCollisionsBeforeMutation`、`TestAggregateTraitResults*` 和 `TestApplyStorageToStatefulSet`，补主容器/init/sidecar 同时存在时的隔离断言。

## O26：私有 Secret 索引维护了不存在的就绪状态

**证据。** [索引结构](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/interfaces/api/assembler/v1/component.go#L81-L90) 为 `componentSecretIndex → componentSecretValues.entries → componentSecretValue{value, ready}`。[唯一生产构造点](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/interfaces/api/assembler/v1/component_credential.go#L160-L188) 固定写 `ready: true`，且 `component` 参数未使用；读取侧却保留 ready 检查和 value 适配函数。

实际需求是为组件列表解析 Secret 的 key/value，并计算响应中的 `Resolved`。多层私有结构让简单的 key 存在性、空值判断看起来像另有异步就绪流程。

**最小方向与约束。** 使用 `map[string]map[string]string`；保留公共 `Resolved`，按 key 存在且值非空计算。保留缺失 Secret、缺失 key、空字符串、存在但无 key 的 Secret 的不同输出，以及 namespace 隔离、同名 first-wins、键排序、递归 source 和输入不被修改。

**后续验收。** 保留组件凭据测试中的 resolved/unresolved、空值、空 Secret、手工类似 Base64 的值和 init/sidecar 场景。不要改变编码策略或凭据展示权限。

## O28：Job runPolicy 的 validator 替换能力没有消费者

**证据。** [applyJobRunPolicy](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/event/workflow/job/job_batch.go#L196-L243) 接收 `validators ...func(*batchv1.Job) error`，先创建默认身份校验，再允许第一个参数覆盖。[即时任务](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/event/workflow/job/job_instant.go#L223-L226) 和 [计划任务](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/event/workflow/job/job_scheduled.go#L144-L148) 传入的仍是相同参数构造的默认校验；延迟和重试入口不传参数。

全仓调用检索核对了唯一显式传 validator 的测试：[job_instant_scheduled_test.go](https://github.com/PixelCores/Eruun/blob/2e0d55b6afb9af2d71e68d92d3afd91b330415b9/pkg/apiserver/event/workflow/job/job_instant_scheduled_test.go#L327-L334) 也传相同校验，没有不同实现或测试替身。私有入口因此多出一个无实际替换需求的扩展点，读者必须检查调用方才能确定校验规则。

**最小方向与约束。** 删除可选参数、覆盖分支和两个生产调用方的重复闭包构造，保留默认 `validateExistingJobExecutionIdentity`。不删除身份校验，不更改旧任务状态、generation 比较、UID 删除条件或等待行为。

**后续验收。** `TestApplyJobRunPolicy*`、`TestExistingJobExecutionIdentityComparesGenerationsWithinOneTask`，覆盖旧任务终态、旧任务活跃、存储查询失败，以及即时/计划/延迟/重试入口。属于小型局部整理，不需要新的策略接口或 IOC 变化。

## 已检查但不建议作为本轮优化目标

- IOC 容器包装、装配入口和 service/repository 契约承担现有依赖管理职责；不合并既有层，不切换注入机制。
- Workflow lease、generation/token fencing、outbox、恢复扫描、Kafka 连续 offset ACK、UID/attempt 删除保护，均有当前可靠性或资源身份契约。
- HTTP/gRPC 适配及 ProtoJSON 转换承担 int64、presence、动态字段和未知字段行为；不能仅按序列化次数判定冗余。
- callback 两侧短规则存在重复，但未知状态政策不同，未证明生产漂移；不为短 switch 增加公共抽象。
- Python 恢复材料先全部校验再写入有明确用途。本轮不提议整套流式恢复改写，也没有将内存集合视为历史 O24 已删除的内容。
- 几行无消费者 helper、未使用参数和纯转发包装可以随相邻修改处理，收益不足以扩大本轮整改范围。

## 覆盖与验证边界

| 范围 | 本轮深度 |
| --- | --- |
| 启动、配置、IOC、角色装配 | 直接阅读主要装配和启动入口，抽查 worker 生命周期、预算 repository；保持架构边界 |
| HTTP/gRPC、DTO/assembler、Domain | 深读组件链接/凭据与 lifecycle reporter 到操作记录；抽查严格请求解析、账户授权、设置、转换和查询 |
| Workflow/Job/Traits | 深读 Trait 数据流、runPolicy 和所有调用方；抽查 Job builder、资源调和、CloudJob、callback、cleanup fence |
| Jobs/Harbor、Infrastructure/utils | 深读 checkpoint 上传、验证、创建和恢复链；抽查结果投递、事务能力、cache、messaging、observer、locker |
| 部署、构建、脚本 | 抽查四角色 Helm 模板、持久化名称保护、安装器凭据处理、Makefile 和 CI |

这是一轮主要模块覆盖、重点路径深入的审查，不是逐行穷尽全部文件。生成 Protobuf 和第三方依赖没有逐行审查；没有真实 MySQL、Redis、Kafka、Kubernetes、ACS、云厂商或仓外 Go 消费者验收。

### 本次执行证据

- 基线固定为 `2e0d55b6afb9af2d71e68d92d3afd91b330415b9`，独立工作树初始干净；本 PR 仅改变此报告和 docs 索引。
- Go 1.27.1，临时 `-overlay` 文件全部位于仓库外。`TestAudit20261005OperationFailureKeepsStructuredFields` 和 `TestAudit20261005ServiceLinkUsesServicePort` 按预期失败，分别证明 O27 和 O22 的当前问题；没有把它们写入或留在源码树中。
- 既有定向测试结果和文档检查见下方最终验证记录。定向测试验证当前相邻契约，不能替代尚未实施的整改验收。
- 未运行全仓 race/vet/build、真实集成或性能测试；本 PR 无 Go/Python/部署行为修改。

### 最终验证记录

以下既有测试合计 **5 个包、48 个不同顶层测试通过，无跳过**：

```sh
go test -p 2 \
  ./pkg/apiserver/domain/service/application \
  ./pkg/apiserver/interfaces/api/assembler/v1 \
  ./pkg/apiserver/event/workflow/job \
  ./pkg/apiserver/workflow/traits \
  ./pkg/apiserver/jobs/artifacts \
  -run '^(TestOperationTaskAtomicWrites|TestLifecycleRecordFailureKeepsKubernetesEffectsExplicit|TestRestartApplicationWorkloadsReturnsErrorWhenMarkRestartingFails|TestStartApplicationDeploymentsTriggersFailureCallbackForPartialFailure|TestConvertComponentModel.*|TestApplyJobRunPolicy.*|TestExistingJobExecutionIdentityComparesGenerationsWithinOneTask|TestApplyTraits.*|TestAggregateTraitResults.*|TestApplyStorageToStatefulSet.*|TestCheckpointMaterialFileManifest)$' \
  -count=1 -v
```

首次执行时 application 回调测试被沙箱禁止监听临时 TCP 端口；其余四包通过。随后在允许本地端口的环境，对相同冻结基线重跑上述四个 application 顶层测试，全部通过。没有因这次环境失败修改代码或测试。

文档交付检查均通过：25 个固定提交源码链接的文件与行号、相对文档链接及锚点、报告/索引范围、`scripts/check-sensitive-content.sh`、`git diff --check`。这些检查不构成运行时或性能验收。
