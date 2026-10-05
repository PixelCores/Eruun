# 全局重复代码审查（2026-10-05）

状态：**Historical / Audit**。本报告记录冻结版本的静态证据，不代表后续 main 的实时状态。

基线：`main@52db123490ac8c8490ca827c63356bce6bfb4337`。从远端 main 创建独立分支；本 PR 只增加扫描工具、报告和索引，不修改运行代码、IOC 注入、整体架构或协议。

## 结论

人工复核确认 **8 组维护性重复**，均为非阻断优化项。结论依据是多个入口重复维护同一规则及其修改成本，而不是相似行数。没有据此证明现存线上故障、性能问题或信息泄露；建议中的边界用例是后续实施要求。

| 编号 | 重复规则 | 最小方向 | 实施关注点 |
| --- | --- | --- | --- |
| D01 | Workflow 部署组件覆盖集合，三处 | application 同包纯函数 | required 集合、错误码仍由调用者管理 |
| D02 | Updating / Restarting 组件状态写入 | 同包私有方法显式接收目标状态 | Cleaning、CAS、错误和失败记录 |
| D03 | 严格 JSON body 解码，两处 | 现有文件共享核心，保留两个语义入口 | 仅首次 EOF 策略不同 |
| D04 | Instant / Scheduled Job 创建和等待 | 现有包内具体 helper 或已有 base 方法 | ownership、对象身份与超时 |
| D05 | 日志与文件/exec 的 Pod、容器解析 | 日志复用现有 resolver | Completed、Follow、入口锁差异 |
| D06 | HTTP / gRPC viewer 字段白名单 | 复用既有 summary DTO 的具体投影 | 协议形状、nil 与角色边界 |
| D07 | callback 终态目标和 method | 既有 callback model 附近的纯规则 | 默认分支、空白 URL、恢复与租约 |
| D08 | adopted StatefulSet 引用 PVC 枚举 | 既有 resourceimport/contract 的纯函数 | ordinal 整数边界、写前重验 |

建议先实施 D01、D03，再分别处理 D02、D04、D05。D06–D08 先补齐两个入口的契约对照再提取。每组可以独立提交小 PR；不将其扩展成统一生命周期、通用控制器或映射框架。

## 可复现扫描及统计口径

在包含本 PR 的 checkout 中执行：

```sh
python3 scripts/scan_duplicate_blocks.py \
  --ref 52db123490ac8c8490ca827c63356bce6bfb4337 \
  > /tmp/eruun-duplicate-blocks.json
```

脚本只需要 Python 3 标准库和 Git。通过 `git ls-tree` / `git show` 读取指定提交，不读取未提交的工作区修改；输出包含解析后的完整 revision 和两侧原始文件行号。默认 `--ref HEAD`；以后扫描新提交时应保存其完整 SHA。扫描自身和报告不属于本次冻结基线。

本次遍历 Git 跟踪文件中的 Go、Python、Shell、SQL、Proto、Dockerfile、Makefile、YAML、JSON、TOML 和 Helm tpl。Markdown、图形文件、LICENSE、gitignore、依赖清单/锁文件及 txt 不作代码克隆匹配。使用以下分类，彼此不交叉比较：

| 类别 | 文件数 | 物理行数 | 文本候选对 |
| --- | ---: | ---: | ---: |
| 手写 Go（非测试） | 435 | 106,565 | 191 |
| Python（非测试） | 11 | 4,067 | 0 |
| Shell（非测试） | 12 | 1,151 | 0 |
| SQL | 2 | 69 | 0 |
| Proto 源文件 | 5 | 1,927 | 2 |
| Dockerfile / Makefile | 5 | 187 | 0 |
| Go 测试 | 406 | 132,707 | 1,843 |
| Python 测试 | 6 | 3,362 | 1 |
| Shell 测试 | 2 | 1,335 | 11 |
| YAML / YML / Helm tpl | 21 | 2,519 | 27 |
| JSON / TOML | 116 | 3,859 | 34 |
| 生成 Go（仅清点，不匹配） | 10 | 26,330 | — |
| 合计 | 1,031 | 284,078 | 2,109 |

物理行数包含空行和注释，不是有效代码行数。测试按 `_test.go`、`_test.sh`、文件名 `test_` 分类；JSON 归入配置类别，其中包含 schema、示例及测试夹具，不代表 116 份运行配置。生成文件按文件头标记或本仓库 `/pb/` 路径识别；本基线对应十个 protobuf Go 绑定。

匹配是文本启发式：跳过空行、整行注释样式和纯标点行，去除剩余空白，以同类别同语言的连续 **8 个保留行**为窗口，再扩展相等区间。同一候选对在同一文件的两侧区间不重叠；同一窗口出现超过 **20 次**时跳过，本次跳过 7 个高频窗口。可通过 `--min-lines`、`--max-occurrences` 改变阈值。

这些是**候选对**，多个 pair 可来自同一组规则或共享片段，不能相加成重复代码行数、缺陷数或重复率。输出既不进入 CI 门禁，也不因发现候选而返回失败。当前脚本不是词法分析器或 AST 检测器：字面量内空白也被删除，类似注释的文本行可能被跳过，行内注释未完整解析；重命名、重排、短于阈值和高频块可能漏检。误报需人工排除；某语言零候选不等于没有语义重复。

## 人工复核覆盖

全局文件清点与上述文本筛选覆盖表中范围；人工工作按模块分工、候选定位和调用链复核，不是全仓逐行审阅，也没有逐对确认所有测试/配置候选。

- **直接逐段核对**：application 版本更新/生命周期/Pod 日志；Workflow/Job 创建等待、callback 两入口、adopted PVC 保护与恢复；HTTP/gRPC 应用列表和严格解码、Jobs 错误/下载入口、DTO 自定义 JSON、assembler Ingress；MySQL schema introspection、workspace 资源组装、配置与启动入口。
- **候选及边界抽样**：resourceimport 合并/RBAC、repository 状态集、typed Kubernetes controller、traits、cloudjob、locker/messaging/observer、utils；Harbor 的 checkpoint/recovery/transport/runner/environment；部署清单和 Helm 模板。
- **仅清点/机械筛选或有限抽样**：未命中的账号/设置 API 和服务函数、全部 SQL driver、cache/importsecret/observability 细节、完整 URL dial 链；所有测试、schema、JSON fixture、示例和非 Go 脚本并未逐段人工审完。生成代码不作手写去重评价。

下面的位置均链接到冻结提交。它们包含少量语义重复（机器无法完全匹配），也排除了机器命中的 import 块、类型字段和不同策略。

## D01：三处计算同一 Workflow 部署组件集合

证据：[deploy-all](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/application_update_version_actions.go#L174-L203)、[Ready coverage](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/application_update_version_actions.go#L231-L257)、[changed-components coverage](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/application_update_version_execution_scope.go#L85-L111)。

三处都跳过 nil/approval step；有 substeps 的父项不贡献组件；按同一 job type 谓词筛选父/子项；组件名 trim/lower、去空并入 set。调用者分别在 add-all 事务内、Ready 观察目标及 changed_components 提交时使用这些集合。步骤执行规则一旦变化，需要同步三份算法，否则相同 Workflow 在不同入口会得到不同覆盖判定。

只提取“已解码 WorkflowSteps → 规范化部署组件 set”。解码、required 来源、缺失项排序与错误仍保留在三个调用者：Ready 缺失列表与 changed_components 的去重/空输入处理不同；错误码也有 ErrExecWorkflow / ErrWorkflowConfig 差异。相邻的初次 Deploying 筛选只接受 Component step，不能直接纳入。

后续验证：保留 add-all/cleanup 父子步骤、Ready 缺覆盖无写入、changed-components 部分覆盖拒绝测试；共享函数覆盖 nil、approval、带 substeps 父项、非部署 substep、大小写/空白重复名。

## D02：Updating 与 Restarting 的组件状态写入

证据：[markComponentsUpdating](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/application_update_version_workflow.go#L18-L58)、[markComponentsRestarting](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/application_update_version_workflow.go#L178-L218)。

两份约 40 行流程相同：空输入短路、目标名 trim/lower 去重、按 appID 查询组件、跳过 nil/未选中/Cleaning、清空 LastAbnormal、调用同一 runtime-field 更新入口。主要差异是目标状态与错误文字。版本更新即时自动执行与应用重启分别调用它们，Cleaning 保护和字段写入规则需要双改。

可用同包私有方法显式接收目标状态，必要时保留语义包装。不新增接口、状态机或包。保留持久化组件名称当前仅 lower 的匹配规则、串行 fail-fast、错误上下文，以及 [runtime fields CAS](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/repository/component_runtime_status.go#L18-L57)。初次部署的 eligibility 与缓存失效不同，不合并整个部署状态流程。

后续验证：Updating 即时/未来 executeAt、重启状态写失败的 operation failure 记录，以及两状态共用的空输入、Cleaning、查询错误、CAS 失败矩阵。

## D03：严格 JSON 请求解析的两份实现

证据：[bindStrictJSON](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/interfaces/api/request_binding.go#L38-L62)、[bindStrictJSONAllowEOF](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/interfaces/api/request_binding.go#L64-L91)。

必填 body 入口和 start/stop/restart 可选 body 入口都创建 decoder、拒绝未知字段、读取首值、拒绝第二个 JSON 值、记录相同错误并返回传入业务码。唯一有意差异是首次 Decode 得到 EOF 时，可选 body 返回零值。调整尾随数据处理或严格解析时需要修改两份代码。

在现有文件内共用解析核心，显式传入首次 EOF 策略，保留两个可读入口；不将普通 Gin binding 或 gRPC ProtoJSON 一并统一。必须保留 logBindErr、invalidErr 和必填 body 拒绝空输入的行为。

后续验证：两个入口的空 body、合法单值、未知字段、截断 JSON、第二值（含 null）矩阵，以及解析失败不调用 service。

## D04：Instant / Scheduled Job 重复创建与等待规则

证据：[Instant createJob / wait](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/event/workflow/job/job_instant.go#L243-L275)、[Scheduled createJob / wait](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/event/workflow/job/job_scheduled.go#L194-L226)。

两组函数正文相同：重查 workflow ownership、构造现存 Job 身份校验、调用 tracked-resource 创建；随后使用相同默认 timeout、JobInfo namespace/name 优先级和 waitForJobCompletion。普通即时、command/eval 及一次性定时 Job 都可到达这些分支。对象替换保护或等待命名规则需要同步两份。

收敛到包内具体 helper 或已有 base 的明确方法即可。保留创建前第二次 ownership 检查，不把上层已检查当作删除理由。两者 Run 编排有实际差异：延迟 checkpoint 与 run policy 顺序、CronJob 分支和 eval 结果保留，不合并整个控制器。

后续验证：保留 stale owner、AlreadyExists replacement、重建等待后对象替换、ownership 读取失败、CronJob 与超时等待用例；对受影响共享状态/ownership 用例运行 race。

## D05：日志重复实现已有 Pod / 容器 resolver

证据：[StreamComponentLogs](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/component_logs.go#L33-L90)、[resolveComponentPodTarget](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/component_pod_ops.go#L166-L228)。

日志重新实现输入规范化、仓储查找、默认 namespace、Pod 选择、Pending/unavailable、容器指定/默认选择；文件和 shell 入口已经复用现有 resolver。两处已经出现 nil component 保护差异，但没有证据证明当前仓储会返回 nil,nil，不将其描述为已复现崩溃。

日志可复用现有 helper 并允许 Completed；在现有私有返回值保留 Pod 选择状态，继续推导 Follow。不得把 I/O 流程一起合并：日志可读 Completed 且 Follow=false；文件/exec 拒绝 Completed；日志不获取写应用锁，exec 保留现有锁；入口专用错误、tail lines、source UID 定位均需保留。

后续验证：同一 Completed Pod 的日志成功且不 Follow、文件/exec 拒绝；Running 日志 Follow；指定/默认容器、最新 Ready、Pending 与 source UID 用例。

## D06：viewer 应用列表白名单在 HTTP / gRPC 各维护一次

证据：[HTTP list](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/interfaces/api/application_lifecycle.go#L91-L105)、[gRPC list](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/interfaces/grpc/applications.go#L75-L89)、[现有 ApplicationSummary](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/interfaces/api/dto/v1/accounts.go#L63-L70)。

两个入口调用同一注入服务，随后独立选择 ID、Name、Namespace、WorkspaceID、Version 五字段。HTTP 使用明确的 Summary，gRPC 重建较宽的 ApplicationBase。可见字段变化需同时修改两处投影及 DTO。当前字段一致，没有发现已证实的额外字段泄露。

优先在现有 assembler 中复用具体的 ApplicationBase → ApplicationSummary 投影；两端保留角色判断和协议封装。实施前验证 Summary 经现有 typed encoder 与 protobuf 响应的兼容性，不新增授权注册表、service interface 或反射 mapper。gRPC 跳过 nil 元素、HTTP 没有此分支；错误返回和 JSON/protobuf 形状也不应被顺手统一。

后续验证：同一完整 fixture 下，两端 viewer 仅保留五字段值，member 不受影响；空列表、nil 元素、分页及错误契约。现有 auth/pagination 测试不能代替字段白名单断言。

## D07：callback 终态目标与 method 重复维护

证据：[Worker selector](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/event/workflow/controller.go#L1570-L1605)、[审批取消/恢复 selector](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/workflow/workflow.go#L2003-L2040)。

明确终态的 success/cancelled/timeout/reject/failure URL fallback 和 method 规范化相同。Worker 正常完成及无 Worker 的审批取消/终态恢复分别调用，最终构造同类 CallbackJobInfo。新增事件或 fallback 规则需要双改。

在已有 WorkflowCallback model 附近集中纯选择规则即可。两 selector 的 default 不同：Worker 返回 failure，service 对非终态返回空；Worker 的真实入口已有终态门禁，不能据此断言实际行为冲突。共享时明确非终态结果并保留入口检查。专用 URL 当前先判断原值非空，再 TrimSpace；不得顺手改变纯空白 URL 的 fallback 行为。

两端租约、取消上下文、parent generation/status/app/workspace 复核和恢复写入各有用途，不合并整段发送编排。后续验证包含终态/非终态、task callback 优先、专用 URL 缺省/空白、method 规范化，以及审批取消与 Worker 两入口回归。

## D08：两层分别枚举 adopted StatefulSet 的引用 PVC

证据：[Application 枚举](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/domain/service/application/application_workload_adopted.go#L447-L482)、[Workflow restart 枚举](https://github.com/PixelCores/Eruun/blob/52db123490ac8c8490ca827c63356bce6bfb4337/pkg/apiserver/event/workflow/job/job_version_restart.go#L355-L387)。

两者收集显式 PVC、按 ordinals.start 与 replicas 展开 VolumeClaimTemplates 名称、去重并排序，用于生命周期与 Workflow 重启的安全检查。命名和 ordinal 修正需要同步两条入口；现有 resourceimport/contract 已承接共享的重启策略校验，可容纳此纯函数，无需创建新层。

这里不能直接宣称两个实现完全等价：一处遍历 offset，一处以 startOrdinal+replicas 作循环上界，int32 溢出边界不同。实施前明确合法范围和边界预期，再统一纯枚举。各层 live GET、UID、replica 来源、PVC terminating/Bound 检查及精确错误继续保留；预检和写前最新对象重验是正确性要求，不是应删重复。

后续验证：nil、无/多 VCT、非零 ordinal、重复显式引用、稳定排序、整数边界，以及两个入口“危险 PVC 无写入”和写前重验用例。

## 保留的重复与暂不提取项

| 范围及例子 | 本次判断 |
| --- | --- |
| protobuf/DTO 主子 step 字段与生成 Go | wire 字段号、类型身份和允许字段不同；生成文件不手改。主子 step 已复用 properties 解码，不新增反射注册表。 |
| typed Kubernetes controller、workspace RBAC CRUD | 对象类型、客户端、身份/roleRef 与重建行为不同；泛化需多组 callback，会遮蔽权限边界。 |
| adopted dependency / Secret 恢复 | Secret 有密文 holder rebase 与 ciphertext/snapshot 原子事务，不能把相似 precheck 扩展成通用持久化框架。 |
| lifecycle stop/start/restart、cleanup fence | 副本保存/恢复、锁内提交、状态失败记录和 identity/graph 条件不同；已有局部 helper，保留显式编排。 |
| cleanup trigger / write-permission JobType 集合 | 后者包含更多 cleanup/reset/restart；概念不同，合并会改变权限或触发范围。 |
| HTTP / gRPC artifact 错误映射、幂等处理 | HTTP 有 MaxBytesError/safe message，gRPC 隐去 parser 回显；真正的 idempotency key 规则已共享，不统一传输上下文。 |
| MySQL table/column introspection、不同锁/MQ adapter | SQL 参数/fallback、存储一致性、ACK、租约不同；不因循环或 context 外壳相似而合并。 |
| Harbor upload/checkpoint/control/recovery | 结果上传、不可变 checkpoint 发布轮询、sandbox admission 的状态与重试语义不同，底层 headers/connection/deadline 已复用；Recovery 两次持久化代表两个阶段。 |
| 静态 stack 与 Helm、多角色 Deployment | 静态清单重复配置和 RBAC 是可见维护成本；Helm 已复用角色模板。保留两种安装入口与不同角色身份，后续用既有安装/渲染验证约束一致性，不为本审查新增生成系统。 |
| 测试 fixture | 相同资源构造可能覆盖 CrashLoopBackOff 与 ContainerCreating、普通 cleanup 与审批后 cleanup 等不同断言。可按可读性共用夹具，不能按候选数删除测试。 |
| 短 map copy/hash、RBAC task metadata | 低收益或机会性整理；后者可考虑函数内 type switch，但须保留 cluster object namespace、DeepCopy 与 share 策略，不列核心八项。 |

本轮对既有 Ingress spec 共享规则、TraitResult、Service links、operation record、checkpoint 和 Job policy 的已落地改动按当前源码核对；没有把历史上已处理的问题重新计入八项。

## 本 PR 验证与限制

- 在冻结提交完整运行扫描，得到上表 inventory、2,109 个候选对和 7 个高频跳过窗口；JSON 输出保留在审查工作区的临时路径，可按命令重建，不提交大体积候选快照。
- 临时 Git fixture 验证通过：跨文件/同文件重复、空白变体、原始行号、source/test 隔离、生成文件排除、高频上限、固定 ref 忽略工作区修改、CLI JSON 输出及非法参数拒绝。独立复核后修复并复验了 Unicode 分隔字符的物理行号和同文件连续三份重复的候选边界。
- 报告源码链接、文档索引、敏感内容检查与 diff 空白检查在提交前校验。
- 本 PR 未改 Go 或部署行为，因此没有重跑 Go/race、installer/Helm 或真实数据库/Kubernetes/HTTP/gRPC 集成。上文列出的测试是后续去重时需要保留和补充的验证，不是本轮通过记录。
