# Go 惯用性与代码质量全仓审计（2026-08-09）

> 状态：Historical / Audit。本文记录基于指定 `master` 提交的一次只读审计；它描述审计时点的代码事实，不代表所有建议都应在同一个 PR 中实现。

## 1. 结论摘要

- 审计基线为 `master` 的 `aaec6307b63c96d3dc4a82ab78032ba8c5470fa2`。
- 覆盖全部 59 个 Go package、551 个 Go 文件和约 18.5 万行 Go 源码；没有抽样排除生产包或测试包。
- `gofmt`、`go vet ./...`、`go test ./... -race -cover` 全部通过，本轮没有确认 P0/P1 级 correctness、data loss、race 或 deadlock 缺陷。
- 五月审计中 Q-001、Q-009 仍未闭环，Q-010 仍为部分解决；Q-003 与 Q-011 随新增能力重新出现集中编排或大测试文件信号。
- 本轮新增 Q-016 至 Q-021，重点不是语法偏好，而是 Go 代码的依赖方向、接口侵入性、构造后有效性、隐式全局状态、context 契约与错误传播。
- 当前代码同样存在值得保留的 Go 风格：窄的可选能力接口、有界 worker、清晰的 goroutine 退出所有权、`errors.Is`/`%w` 错误链和以小函数维持的工作流编排。

本报告中的“高/中/低”是治理顺序，不是线上故障严重级别。只把能由当前代码、调用方或测试证明的事项列为行动项；文件长、命名偏好或 `interface{}`/`any` 选择本身不构成发现。

## 2. 审计基线与范围

| 项目 | 结果 |
| --- | --- |
| 审计日期 | 2026-08-09 |
| Git 基线 | `aaec6307b63c96d3dc4a82ab78032ba8c5470fa2` |
| Module | `Eruun` |
| `go.mod` 语言/工具链 | Go `1.25.0`，`toolchain go1.25.8` |
| 实际执行工具链 | `go1.26.2 darwin/arm64` |
| Go package | 59 |
| Go 文件 | 551：生产 332，测试 219 |
| Go 行数 | 185,232：生产 84,391，测试 100,841 |
| 生成文件 | 未发现带 `Code generated ... DO NOT EDIT.` 标记的 Go 文件 |

### 2.1 包含范围

- `cmd/` 中的 Go API Server 入口与启动生命周期。
- `pkg/apiserver/` 下 domain、event/workflow、infrastructure、interfaces、security、utils 与 trait/naming 实现。
- `deploy/` 中以 Go 测试实现的 manifest 验证。
- 所有 `*_test.go`，包括 fake、fixture、并发测试和集成测试入口。
- `version/version.go` 仅作为审计对象读取，没有修改版本。

### 2.2 明确不包含

- Shell、YAML、Dockerfile、前端或部署清单本身的风格审查。
- 真实 MySQL、Redis、Kafka 或 Kubernetes 集群的负载、故障注入与性能基准。
- 本轮只记录问题，不修改生产或测试行为。

## 3. 判定标准与审计方法

审计以当前代码和测试为事实源，项目基线以 `project-style-guide.md` 为准，并参考 Go 官方资料：

- [Effective Go](https://go.dev/doc/effective_go)：小接口、控制流、命名、错误与并发的语言惯例。
- [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)：接口归属、context、错误字符串、goroutine 生命周期等评审约定。
- [Go Concurrency Patterns: Context](https://go.dev/blog/context)：取消、deadline 与请求范围值的传播。
- [Go Concurrency Patterns: Pipelines and cancellation](https://go.dev/blog/pipelines)：goroutine 退出、channel 所有权与取消。

执行了两阶段检查：

1. **全量机器扫描**：对所有 tracked Go 文件统计 package、生产/测试文件、行数、函数跨度、分支密度信号、接口方法数、`init`、package variable、goroutine、nil context fallback、反射注入、跨层 import 与日志形态。
2. **逐包人工复核**：回到命中代码、调用方、接口消费者和测试，确认它是可操作问题还是合理的状态机、兼容逻辑、后台生命周期或测试 fixture。自动指标只用于排序，不直接作为结论。

重点检查面：

- 简洁控制流：入口是否只做编排，校验、计划、提交与展示是否可分离。
- 边界与接口：接口是否由消费者定义且足够窄，domain 是否被 transport/storage 细节侵入。
- 错误与资源：错误是否显式返回并保留 identity，资源是否在所有路径释放。
- 并发：goroutine 是否有 owner、退出条件、等待或有界队列，是否传播 context。
- 数据与 API：nil/empty、默认值、DTO、DB、cache 与 workflow 状态是否保持显式契约。
- 测试：是否可按场景定位、是否需要实现与被测行为无关的巨大 fake。

## 4. 验证记录

已执行：

```bash
go version
gofmt -l $(git ls-files '*.go')
go vet ./...
go test ./... -race -cover
```

结果：

- `gofmt -l` 无输出。
- `go vet ./...` 通过。
- `go test ./... -race -cover` 在允许本地临时端口的环境全部通过，没有 race report。
- 核心包 statement coverage：application 77.0%、namespaceimport 78.6%、domain workflow 74.4%、event/workflow 77.9%、workflow/job 72.2%、interfaces/api 77.7%、traits 73.2%。
- `staticcheck` 与 `gocyclo` 本机未安装；仓库已有对应 Make target，本轮没有安装新工具，也没有把自定义分支信号冒充 `gocyclo` 结果。

测试需要 `httptest` 和 `miniredis` 监听本地临时端口；受限沙箱会返回 `listen ... operation not permitted`，因此全量 race/coverage 结果来自允许 loopback listener 的同一工作区环境。

## 5. Package 覆盖矩阵

下表覆盖 `go list ./...` 返回的全部 59 个 package。LOC 是审计时点的物理行数，只用来说明覆盖范围和定位热点。

| Package | 生产文件 | 测试文件 | 生产 LOC | 测试 LOC |
| --- | ---: | ---: | ---: | ---: |
| `cmd` | 1 | 0 | 16 | 0 |
| `cmd/server/app` | 1 | 1 | 158 | 51 |
| `cmd/server/app/options` | 2 | 0 | 43 | 0 |
| `deploy` | 0 | 1 | 0 | 53 |
| `pkg/apiserver` | 1 | 3 | 1,374 | 2,315 |
| `pkg/apiserver/config` | 12 | 5 | 1,163 | 354 |
| `pkg/apiserver/domain/adoption` | 2 | 1 | 297 | 111 |
| `pkg/apiserver/domain/model` | 15 | 9 | 1,474 | 360 |
| `pkg/apiserver/domain/repository` | 9 | 8 | 1,364 | 1,280 |
| `pkg/apiserver/domain/service` | 1 | 0 | 82 | 0 |
| `pkg/apiserver/domain/service/application` | 47 | 30 | 16,256 | 25,057 |
| `pkg/apiserver/domain/service/conversion` | 5 | 2 | 2,082 | 1,344 |
| `pkg/apiserver/domain/service/internal/cancelsignal` | 1 | 0 | 33 | 0 |
| `pkg/apiserver/domain/service/internal/schedulelock` | 1 | 1 | 91 | 35 |
| `pkg/apiserver/domain/service/internal/traitvalidation` | 1 | 0 | 525 | 0 |
| `pkg/apiserver/domain/service/namespaceimport` | 4 | 11 | 6,740 | 5,466 |
| `pkg/apiserver/domain/service/programminglanguage` | 1 | 1 | 255 | 412 |
| `pkg/apiserver/domain/service/systemsetting` | 1 | 1 | 621 | 994 |
| `pkg/apiserver/domain/service/validation` | 6 | 7 | 1,908 | 5,071 |
| `pkg/apiserver/domain/service/workflow` | 4 | 3 | 2,667 | 4,508 |
| `pkg/apiserver/domain/spec` | 11 | 4 | 1,231 | 309 |
| `pkg/apiserver/event` | 1 | 1 | 52 | 87 |
| `pkg/apiserver/event/workflow` | 17 | 12 | 5,621 | 7,483 |
| `pkg/apiserver/event/workflow/approvaltimeout` | 1 | 1 | 77 | 53 |
| `pkg/apiserver/event/workflow/cloudjob` | 2 | 1 | 99 | 110 |
| `pkg/apiserver/event/workflow/cloudjob/aliyun` | 13 | 1 | 1,763 | 1,617 |
| `pkg/apiserver/event/workflow/cloudjob/contracts` | 2 | 0 | 152 | 0 |
| `pkg/apiserver/event/workflow/cloudjob/custom` | 4 | 1 | 177 | 94 |
| `pkg/apiserver/event/workflow/job` | 51 | 44 | 19,141 | 22,527 |
| `pkg/apiserver/infrastructure/adoption` | 2 | 2 | 828 | 655 |
| `pkg/apiserver/infrastructure/clients` | 4 | 1 | 648 | 625 |
| `pkg/apiserver/infrastructure/datastore` | 1 | 0 | 204 | 0 |
| `pkg/apiserver/infrastructure/datastore/mysql` | 2 | 1 | 298 | 193 |
| `pkg/apiserver/infrastructure/datastore/sql` | 2 | 2 | 436 | 254 |
| `pkg/apiserver/infrastructure/datastore/sqlnamer` | 1 | 0 | 75 | 0 |
| `pkg/apiserver/infrastructure/informer` | 3 | 2 | 1,586 | 1,528 |
| `pkg/apiserver/infrastructure/locker` | 5 | 1 | 836 | 403 |
| `pkg/apiserver/infrastructure/messaging` | 3 | 3 | 850 | 1,232 |
| `pkg/apiserver/infrastructure/observability` | 1 | 0 | 49 | 0 |
| `pkg/apiserver/interfaces/api` | 20 | 27 | 2,473 | 8,224 |
| `pkg/apiserver/interfaces/api/assembler/v1` | 2 | 1 | 960 | 1,003 |
| `pkg/apiserver/interfaces/api/auth` | 6 | 4 | 1,073 | 735 |
| `pkg/apiserver/interfaces/api/dto/v1` | 8 | 2 | 1,894 | 493 |
| `pkg/apiserver/interfaces/api/middleware` | 7 | 6 | 566 | 840 |
| `pkg/apiserver/security/importsecret` | 1 | 1 | 342 | 143 |
| `pkg/apiserver/security/urlpolicy` | 1 | 1 | 122 | 145 |
| `pkg/apiserver/utils` | 6 | 3 | 928 | 1,003 |
| `pkg/apiserver/utils/async` | 1 | 1 | 110 | 174 |
| `pkg/apiserver/utils/bcode` | 7 | 1 | 386 | 191 |
| `pkg/apiserver/utils/cache` | 4 | 1 | 353 | 185 |
| `pkg/apiserver/utils/container` | 1 | 0 | 55 | 0 |
| `pkg/apiserver/utils/errhandler` | 1 | 1 | 54 | 53 |
| `pkg/apiserver/utils/kube` | 5 | 4 | 1,166 | 1,062 |
| `pkg/apiserver/utils/profiling` | 2 | 0 | 57 | 0 |
| `pkg/apiserver/workflow` | 3 | 0 | 62 | 0 |
| `pkg/apiserver/workflow/naming` | 1 | 1 | 181 | 95 |
| `pkg/apiserver/workflow/signal` | 1 | 1 | 212 | 144 |
| `pkg/apiserver/workflow/traits` | 13 | 3 | 2,122 | 1,770 |
| `version` | 1 | 0 | 3 | 0 |

## 6. 值得保留和复用的 Go 风格

### 6.1 大流程保留薄入口，小函数表达阶段

`pkg/apiserver/event/workflow/job_builder.go:33-88` 的 `GenerateJobTasks` 已从旧审计中的 300 多行收敛为清楚的编排入口：加载输入、过滤 execution scope、构建 step、补 adopted dependency、合并 cleanup、应用 failure policy。复杂性仍存在，但被放在有业务名称的 package-local helper 中。

```go
stepGroups := buildWorkflowStepExecutionGroups(...)
stepGroups, err = augmentAdoptedDependencyJobs(...)
cleanupExecutions := buildPersistedCleanupExecutions(...)
executions, totalJobs := mergeStepExecutionsWithCleanup(...)
applyWorkflowFailurePolicyToExecutions(executions, failurePolicy)
```

这是 Q-001、Q-003 后续治理应复用的形态：移动稳定阶段，不引入新的运行期框架。

### 6.2 用窄的可选接口表达能力

`pkg/apiserver/infrastructure/datastore/datastore.go:187-204` 用 `ConditionalCompareAndSwap`、`Transactional` 表达可选能力，调用方可以按需要做 type assertion，而不迫使每个 datastore 实现所有扩展行为。这比继续扩大主 `DataStore` 更符合 Go 的小接口原则。

### 6.3 goroutine 有明确 owner、停止信号和等待点

- `pkg/apiserver/workflow/signal/cancel.go:25-35,95-98,121-132`：`CancelWatcher` 自己持有 `stopCh`、`sync.Once` 与 `WaitGroup`，`Stop` 关闭并等待。
- `pkg/apiserver/infrastructure/informer/waiter.go:134-166`：构造时启动 retry goroutine，`Close` 关闭 signal、释放 executor 并等待。
- `pkg/apiserver/event/workflow/job/job.go:714-767`：worker pool 固定并发数，producer 负责关闭 channel，`WaitGroup` 追踪 job 完成，并可在失败时 cancel。
- `pkg/apiserver/utils/async/executor.go:19-109`：队列和 worker 数量有界，提交支持 context，关闭由 `sync.Once` 管理。

全仓 49 处生产 goroutine 启动点经归类复核，没有发现可以仅凭静态证据确认的无界 fan-out 或永久泄漏；剩余主要问题是 Q-020 中 context 根节点与 nil 契约不统一。

### 6.4 错误 identity 在关键状态路径中得到保留

repository、workflow、API error mapping 大量使用 `errors.Is(err, datastore.ErrRecordNotExist)`，跨层包装普遍使用 `%w`。这让“记录不存在”“状态漂移”“取消/超时”等控制分支不依赖错误字符串，应该继续保持。

## 7. 旧审计 Q-001 至 Q-015 复核

| ID | 当前状态 | 2026-08-09 证据与结论 |
| --- | --- | --- |
| Q-001 | 仍存在 | `namespaceimport/namespace_import.go` 3,570 行、`adopted_import.go` 2,694 行；`ImportNamespaceResources` 378 行，`assignResourcesToApps` 249 行。扫描虽已拆出，但计划、映射、共享资源与 apply 仍集中。 |
| Q-002 | 已解决 | `GenerateJobTasks` 为 56 行左右的编排入口，步骤、cleanup、adopted dependency 和 component job 已拆入同包 helper。 |
| Q-003 | 重新出现 | `updateVersionUnlocked` 已增长到 336 行，同时承担请求归一化、组件/资源动作约束、PVC preflight、workflow 选择、事务路径、非事务路径、callback 与响应。原 action helper 仍复用，但入口再次过载。 |
| Q-004 | 已解决 | validation 已按 application、workflow、trait、service、ingress 等主题分文件；当前最大生产函数 `validateWorkflowSteps` 约 130 行，属于规则密集路径，未再出现原单文件聚合形态。 |
| Q-006 | 已解决 | template clone 的入口、rewrite、override、trait rewrite 仍在独立文件中，并有聚焦测试。 |
| Q-007 | 已解决（契约保留） | conversion 已分为 pipeline、traits、RBAC 等文件，warning + skipped 的 best-effort 契约有测试。`kube_convert.go` 仍较长，但没有仅因行数重新打开该项。 |
| Q-008 | 已解决（继续观察） | 原 cleanup discovery/delete/reporting 已拆分；新增的 StatefulSet pod cleanup 文件较长，但由多个小 helper 表达状态机，当前没有证据表明应重新合并或引入新抽象。 |
| Q-009 | 仍存在 | `server.go` 从约 936 行增长到 1,374 行；`buildIoCContainer` 181 行，文件还包含 bootstrap、leader election、worker drain、queue metrics、replica watcher 与 status sync。 |
| Q-010 | 部分解决 | DTO/assembler 已有 workflow、settings、OAuth、version 等拆分，但 `dto/v1/types.go` 913 行、`assembler/v1/do2dto.go` 856 行，component/resource/credential surface 仍聚合。 |
| Q-011 | 重新出现 | 最大测试文件已重新达到 4,487、3,952、3,860 行；新 adoption/version/workflow 场景持续堆入大文件，局部运行与 fake 维护成本回升。 |
| Q-012 | 已解决 | `gofmt -l` 无输出。 |
| Q-013 | 已解决（环境限制） | Makefile 已有 `staticcheck`、`gocyclo`、`quality` target；当前机器未安装前两者，不属于仓库入口缺失。 |
| Q-014 | 已解决 | 当前行动映射文档存在并在本 PR 同步更新。 |
| Q-015 | Obsolete / 无当前证据源 | `fallback-degradation-audit.md` 已不在仓库中，只有旧审计和旧索引继续引用它；无法验证其中 F-009 至 F-023。当前 fallback 必须回到具体 Current 文档与代码重新建立证据，不能继续引用失踪清单。 |

## 8. 当前行动项总览

| ID | 治理优先级 | 主题 | 最小行动边界 |
| --- | --- | --- | --- |
| Q-001 | 高 | Namespace import 入口与资源归属推断集中 | 同包移动纯计划/推断 helper，不改 import 契约 |
| Q-003 | 高 | Version update 入口重新过载 | 分离 prepare/validate/commit 阶段，复用现有 request/model |
| Q-009 | 中 | Server composition 与生命周期混在单文件 | 先按 assembly、leader、worker、status sync 拆文件 |
| Q-010 | 低 | DTO/assembler surface 聚合 | 只在原 package 内按 component/resource/credential 移动 |
| Q-011 | 低 | 测试文件重新膨胀 | 只按行为主题移动 test/fake，不改断言 |
| Q-016 | 高 | Domain 反向依赖 transport DTO/assembler | 单条 use case 试点，把组装留在接口层 |
| Q-017 | 中 | Datastore interface 侵入 domain model 且查询 stringly typed | 先在一个 repository 增加 typed intent，保留兼容 datastore |
| Q-018 | 高 | 宽 producer interface + 空构造函数 + 反射注入 | 消费者定义窄接口；核心服务构造后即有效 |
| Q-019 | 中 | 全局 registry / `init` 隐式状态 | 内建集合显式构造；插件注册单独保留 |
| Q-020 | 中 | nil context 被静默替换为 Background | 明确 root context，只在进程边界创建；内部拒绝 nil |
| Q-021 | 中 | `JSONStruct` helper 吞序列化错误 | 增加显式 error-returning 编码路径并逐调用点迁移 |

## 9. 详细发现

### Q-001：Namespace import 的计划、归属推断与应用编排仍集中

**证据**

- `pkg/apiserver/domain/service/namespaceimport/namespace_import.go:226`：`ImportNamespaceResources` 378 行。
- `pkg/apiserver/domain/service/namespaceimport/namespace_import.go:1526`：`assignResourcesToApps` 249 行。
- 同一个入口依次处理 mode、management mode、kind、扫描、已有应用索引、adopted replay/planning、资源分组、应用创建、label patch 和 response。

```go
if managementMode == config.ManagementModeAdopted {
    adoptedPlanning, replayAppID, adoptedReplay, err = s.tryAdoptedApplyReplay(...)
    if err == nil && !adoptedReplay {
        adoptedPlanning, err = s.buildAdoptedImportPlanning(...)
    }
}
```

`assignResourcesToApps` 又在一个循环中组合 cluster scope、显式 label、名称解析、component name 与 warning precedence，随后继续做 workload reference 和共享资源归属。

**为什么不够 Go-idiomatic**

入口方法不再是“读起来像流程”的编排器；局部变量和状态 flag 承担了隐式阶段对象的职责。修改一种 precedence 时，需要同时理解 scan、shared app、adopted 与 apply 路径。

**最小改进**

- 先提取无副作用的 resource identity/ownership inference 和 warning 生成函数。
- 复用现有 `importAppPlan`、`adoptedImportPlanning`，不要新增通用 planner 框架。
- apply、patch 和 persistence 保持在现有 service，直到纯计划阶段稳定。

**风险与验证**

高契约风险：必须保持 label precedence、cluster-scoped 归属、plan fingerprint、replay 和 warning 顺序；运行 namespaceimport 全包测试并比较 response fixture。

### Q-003：Version update 的主入口再次成为多阶段状态机

**证据**

`pkg/apiserver/domain/service/application/application_update_version.go:38` 的 `updateVersionUnlocked` 336 行，包含：

```go
normalReq.Components, err = normalizeVersionUpdateJobFailurePolicies(...)
if err := validateVersionUpdateActionContract(...); err != nil { ... }
normalReq.Components, err = preflightVersionUpdateStatefulSets(...)
requiresWorkflowStepSync, err := hasWorkflowStructureChanges(...)
requiresAutoExecWorkflow := autoExec && (...)
```

之后同一函数继续选择 auto-exec/noop callback/direct commit 三条持久化路径、同步 workflow step、附加 operation task 并组装 API response。

**为什么不够 Go-idiomatic**

错误处理虽然是清楚的 early return，但函数把“准备计划”“校验契约”“选择 workflow”“提交”“展示”五个阶段压在一个词法作用域中。简单语法无法抵消过长的状态组合。

**最小改进**

- 以 package-local helper 分离 request normalization/preflight、workflow selection 和 response/task finalization。
- 优先传递现有 DTO、model、map 和 resource action 类型；只有多个 helper 确实共享不变量时才引入一个私有 plan struct。
- auto-exec transaction 与非 auto-exec 行为保持原样，不在纯重构中改变 no-op、callback、PVC 或 cleanup 语义。

**风险与验证**

高风险：覆盖 version update、autoexec、adopted、StatefulSet PVC、callback 和 transaction rollback 测试，并运行 application 全包 race test。

### Q-009：Server composition、leader 生命周期和运行期 worker 仍由一个文件承载

**证据**

- `pkg/apiserver/server.go` 1,374 行。
- `buildIoCContainer` 位于 `server.go:181`，约 181 行，初始化 datastore、cache、三类 queue、locker、Kubernetes client、informer、repository、service、API 与 event worker。
- 同文件继续实现 bootstrap setting、leader election retry/release、worker drain、queue metrics、replica watcher 和 component status sync。

```go
services := service.InitServiceBean()
for _, svc := range services {
    if err := s.beanContainer.Provides(svc); err != nil { ... }
}
eventWorkers := event.InitEvent()
```

**为什么不够 Go-idiomatic**

启动顺序需要在单个长文件中靠阅读位置推断，构造、运行和 shutdown 的职责没有形成可独立验证的 Go 函数/文件边界。

**最小改进**

只做同 package 文件拆分：assembly、bootstrap、leader lifecycle、worker lifecycle、status sync；先不替换容器，不新建“framework” package。

### Q-010：DTO 与 assembler 仍按历史聚合文件增长

**证据**

- `pkg/apiserver/interfaces/api/dto/v1/types.go` 913 行。
- `pkg/apiserver/interfaces/api/assembler/v1/do2dto.go` 856 行。
- component、resource detail、secret-derived credential、link 等转换仍共享一个文件；这些 surface 已有清楚的私有 helper，说明可以只移动文件而不改变逻辑。

**最小改进**

在原 package 内按 `component`、`resource`、`credential/link` 移动类型和 helper；不改导出符号、JSON tag、nil/empty 或 secret masking。

### Q-011：测试覆盖很好，但按场景定位的文件边界重新失守

**证据**

| 文件 | 行数 |
| --- | ---: |
| `interfaces/api/workflow_test.go` | 4,487 |
| `domain/service/application/application_version_autoexec_test.go` | 3,952 |
| `domain/service/workflow/workflow_test.go` | 3,860 |
| `event/workflow/workflow_test.go` | 2,990 |
| `event/workflow/job/job_adopted_source_test.go` | 2,910 |

Q-018 的宽接口也放大了 fake：例如 API workflow test 为满足 `ApplicationsService` 编译期断言，需要实现许多与当前场景无关的方法。

**最小改进**

- 按 cancel/approval/schedule/status、autoexec/cleanup/PVC/callback 等行为拆测试文件。
- 把只服务一个 surface 的 fake 留在该测试文件；共享 fake 只保留真正共享的状态。
- 只移动，不改 fixture、并发时序或 assertion。

### Q-016：Domain service 反向依赖 API DTO 与 assembler

**证据**

- 53 个生产 Domain Go 文件 import `pkg/apiserver/interfaces/api/dto/v1`。
- 4 个生产 Domain Go 文件直接 import `interfaces/api/assembler/v1`：application 主服务、query、component convert、namespace import。
- `ApplicationsService` 的输入输出直接包含大量 `apisv1.*Request/Response`。

```go
import assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
import apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"

type ApplicationsService interface {
    CreateApplications(context.Context, apisv1.CreateApplicationsRequest) (*apisv1.ApplicationBase, error)
    // ...
}
```

**为什么不够 Go-idiomatic**

依赖方向从 domain 指回 transport，HTTP DTO 变化会迫使业务服务与测试同步变化；assembler 也不再只是接口层展示逻辑。接口因此既表达业务能力，又携带 wire contract。

**最小改进**

- 选择一条窄、低风险路径试点，让 domain 接收/返回 domain-owned command/result 或 model。
- DTO -> command 与 result -> DTO 留在 handler/assembler。
- 不先建立完整 usecase framework；只有第二个消费者出现时再提取共享应用层类型。

**风险与验证**

跨层兼容风险：必须固定 JSON、错误码、nil/empty、pagination 与 cache 行为。按 route -> DTO -> assembler -> service -> persistence -> tests -> docs 跟踪单条链路。

### Q-017：Datastore interface 迫使 Domain model 实现持久化细节

**证据**

`pkg/apiserver/infrastructure/datastore/datastore.go:83-90`：

```go
type Entity interface {
    SetCreateTime(time.Time)
    SetUpdateTime(time.Time)
    PrimaryKey() string
    TableName() string
    ShortTableName() string
    Index() map[string]interface{}
}
```

每个 domain model 必须实现 table/index/time mutator；`DataStore` 还暴露 `IsExistByCondition(table string, cond map[string]interface{}, dest interface{})`、string condition field 与 update map。

**为什么不够 Go-idiomatic**

这是典型的侵入式 provider-owned interface：业务实体为了通用 datastore 满足存储协议，查询字段和更新形态到运行时才校验。相比之下，repository 中的 `FindByID`、`FindByAppID` 更能表达业务意图并获得编译期检查。

**最小改进**

- 不直接替换主 datastore；先让一个高频调用通过现有 repository 的 typed method 表达 intent。
- 新增 datastore 扩展时继续使用小的 consumer capability interface，避免扩大 `DataStore`。
- 只有在 repository 足以承接映射后，才评估把 table/index 从 domain model 移到 infrastructure adapter。

**风险与验证**

高持久化风险：表名、primary key、CAS、事务、rows affected、GORM tag 和错误 identity 都必须保持。

### Q-018：宽接口和空构造函数让依赖只在反射 Populate 后才有效

**证据**

- `ApplicationsService` 31 个方法，`WorkflowService` 18 个方法。
- namespace import 只调用前者的 2 个方法；event/workflow 只调用后者的 5 个方法。
- 生产代码有 69 个 `inject` 字段，分布在 20 个文件。
- `NewApplicationService()` 与 `NewWorkflowService()` 都返回依赖为空的实现：

```go
func NewApplicationService() ApplicationsService {
    return &applicationsServiceImpl{}
}
```

对象只有在 `buildIoCContainer` 最后的反射 `Populate()` 后才成为有效值；构造函数名字无法保证其返回值可以安全使用。

**为什么不够 Go-idiomatic**

Go 接口通常由消费者按所需行为定义；构造函数通常应建立不变量。当前做法让 fake 被迫实现不相关方法，也把缺失依赖从编译期推迟到运行期或 nil dereference。

**最小改进**

- namespace import 定义只含 create 两方法的私有 consumer interface；event/workflow 定义运行时所需的五方法接口。
- 为一个核心 service 试点显式构造函数参数，并在 server assembly 处创建；不要同时迁移所有 bean。
- 兼容 facade 可保留到调用方迁移完成，避免一次性 package 重排。

### Q-019：多套全局 registry 与 `init` 带来隐式进程状态

**证据**

- 生产代码共有 12 个 `init()`；其中多个 model 文件在 import 时写入 `registeredModels`。
- cloud provider 在 `init()` 中注册内建 provider。
- API 与 trait processor 虽改为显式 `InitAPIBean`/`RegisterAllProcessors`，仍写入 package-global slice/map，并需要 `Reset*ForTest` 恢复测试隔离。

```go
func init() {
    RegisterModel(&Applications{}, &ApplicationComponent{})
}

func init() {
    registerBuiltinCloudProviders()
}
```

**为什么不够 Go-idiomatic**

import 产生可变状态，初始化顺序、重复注册和测试并行性需要 mutex/once/reset API 共同维护。内建列表本来是静态依赖，却被建模为运行期注册系统。

**最小改进**

- 内建 model/API/trait/provider 使用显式返回 slice/map 的 factory；server assembly 持有快照。
- 真正需要扩展的 provider registry 保留为独立对象并注入，不把所有内建对象也走全局插件路径。
- 一次只迁移一套 registry，先写顺序、重复项和 test isolation 回归。

### Q-020：内部 API 静默接受 nil context

**证据**

至少 9 个生产文件、20 个位置使用以下模式：

```go
if ctx == nil {
    ctx = context.Background()
}
```

分布在 server leader lifecycle、workflow worker/controller、task lease、HTTP helper、async executor、Kafka 和 cloudjob context helper。

**为什么不够 Go-idiomatic**

Go 官方约定不应传递 nil `context.Context`。callee 静默替换会切断调用方的 cancellation、deadline 与 tracing，同时隐藏错误 caller。真正的进程 root context 应只在 `main`/server lifecycle 边界创建。

**最小改进**

- 先标记哪些函数确实是 root lifecycle constructor，哪些是内部调用。
- root 只创建一次 `Background()`；内部函数要求非 nil 并让调用方传递。
- shutdown/persistence 需要脱离已取消请求时，继续使用有明确 timeout 和注释的独立 context，不机械替换。

**风险与验证**

并发行为风险：错误迁移可能让持久化、unlock、callback 在请求取消后提前终止。逐调用方检查 owner、deadline、terminal write 与 goroutine exit，并运行 race test。

### Q-021：`JSONStruct` 的便利方法吞掉可恢复错误

**证据**

`pkg/apiserver/domain/model/model.go:122-151`：

```go
func (j *JSONStruct) JSON() string {
    b, err := json.Marshal(j)
    if err != nil {
        klog.Errorf("json marshal failure %s", err.Error())
    }
    return string(b)
}
```

`RawExtension()` 也只记录日志并返回 nil。多个生产调用方再把 `raw.JSON()` 转回 `[]byte` 并 `json.Unmarshal`，此时原 marshal error 已丢失，只能得到二次 decode error；`deepCopy` 反射 helper 目前没有调用方。

**为什么不够 Go-idiomatic**

可失败的数据转换应把 error 返回给决定策略的调用层，而不是由 domain value object 记录日志并返回零值。`string -> []byte -> unmarshal` 也增加了无意义的转换和错误位置漂移。

**最小改进**

- 提供 `Bytes() ([]byte, error)` 或直接实现明确的 marshal helper；生产调用方逐个迁移并包装操作上下文。
- 保留现有 `JSON()` 作为兼容层，直到所有可能失败的生产调用点迁移；不要在纯重构中改变持久化 JSON 形态。
- 在确认无反射调用后删除未使用的 `deepCopy`，无需引入通用 copy abstraction。

## 10. 非独立行动项的横向信号

- 生产代码仍有约 253 处 `klog.Infof/Errorf/Warningf`，结构化 `InfoS/ErrorS` 约 120 处。它影响字段查询一致性，但不值得做一次全仓机械替换；按触达文件迁移并避免重复日志即可。
- `interface{}` 改写为 `any` 只有语法收益，不应占用独立 PR；先治理 Q-017 的弱类型边界更有价值。
- 大文件不等于坏设计。`job_cleanup_resources_statefulset_pods.go` 虽长，但拆成多个状态机 helper；本轮没有只凭 LOC 提出重写。
- `module github.com/PixelCores/Eruun` 是 0.1.0 起的规范 module path；仓库内 import 必须保持一致。

## 11. 后续小 PR 顺序

1. **低风险卫生**：Q-011 测试拆分、Q-010 DTO/assembler 同包移动、Q-021 未使用 helper 清理。
2. **同包纯重构**：Q-001 资源归属/计划 helper、Q-003 version update prepare/commit helper、Q-009 server 同包文件拆分。
3. **跨层契约试点**：Q-016 选择单条 API 链路移除 domain -> DTO/assembler 依赖；Q-018 同时为该链路引入窄 consumer interface 与有效构造函数。
4. **持久化边界**：Q-017 选择一个 repository 把 string/map query 收敛为 typed intent，保留 datastore compatibility。
5. **并发和初始化高风险**：Q-020 context owner 迁移、Q-019 registry 实例化；分别验证 shutdown、callback、worker、测试并行性与 race。

每个后续 PR 都应保持行为边界单一。特别是 Q-001、Q-003、Q-016 至 Q-021，不应把 strict/loose、fallback、nil/empty、错误码、事务或 callback 时机混入结构重构。

## 12. 本审计 PR 的边界

- 只新增本文并同步文档索引/行动映射。
- 不修改 `version/version.go`；docs-only 变更不触发 `EruunVersion` 升级。
- 不新增 devlog；本文就是该时点的审计事实与后续路线入口。
