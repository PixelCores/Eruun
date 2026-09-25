# Harbor Runtime 第二阶段：基于文件系统 Checkpoint 的任务恢复计划

> 状态：Draft / Proposal。阶段一代码基线为 `5a3d75b`（PR #59）；本阶段已加入固定版本能力实验和可选恢复实现。当前实现及冻结契约见 [Harbor 评测恢复](harbor-runtime-recovery.md)；本文保留设计目标，真实 ACS 验收仍未执行。

## 1. 目标、依赖与边界

同一个业务任务在 Runner、Sandbox 或节点故障后，启动新的执行实例，从最近一个完整且已确认的恢复点继续。用户已确认接受新进程从文件中的任务进度恢复，不要求恢复内存、原进程、TCP 连接或原 Pod 身份。

Eruun 部署在 ACK 内管理同一集群；任务环境按需创建，使用支持 Checkpoint 的 ACS Agent Sandbox，不依赖 SandboxSet 预热池。工作负载是 Harbor eval 的 Runner 加任务环境，可能运行数周。第一阶段负责至少 1 万个 Harbor Job 并发的稳定运行、共享观察、限速、长期执行及控制面接管；第二阶段不另建调度器、队列或通用恢复平台。

实施复用第一阶段的任务—执行—trial—Sandbox 身份关联、资源创建限速、状态观察和清理契约。第一阶段已随 PR #59 合入 `main`，本阶段以该依赖为实现基线；第二阶段通过验收后再合入 `main`。本轮不创建实施子 PR。

范围包括一致恢复点、Runner 状态持久化、快照与克隆、恢复执行身份、恢复准入及保留清理。跨集群/地域迁移、任意 Harbor Agent 自动恢复、进程内存恢复、外部服务数据快照和外部副作用的通用 exactly-once 均不在本阶段承诺内。允许恢复的 Harbor/Agent/任务组合必须逐一取得实验证据；不满足协议的任务明确标记不支持恢复。

## 2. 已核实事实与实施门禁

| 事实 | 代码或资料 | 设计影响 |
| --- | --- | --- |
| `WorkflowQueue` 持有任务状态、空间及执行租约；`JobInfo` 持有执行记录与内部状态 | [`workflow_queue.go`](../pkg/apiserver/domain/model/workflow_queue.go)、[`job.go`](../pkg/apiserver/domain/model/job.go) | 沿用业务事实源与事务边界 |
| Worker ownership 与既有资源执行身份可以不同，`OwnerRunGeneration` 已与 `RunGeneration` 区分 | [`job.go`](../pkg/apiserver/domain/model/job.go)、[`workflow_lease.go`](../pkg/apiserver/domain/repository/workflow_lease.go) | 接管健康执行不能等同于创建新恢复执行 |
| Runner `/work` 使用 `EmptyDir`；采集状态和 outputs 在 Runner；启动创建新目录并调用 `harbor run` | [`builder.go`](../pkg/apiserver/jobs/builder.go)、[`runner.py`](../runners/harbor/runner.py) | Sandbox 快照不覆盖整个 Harbor Job；可选恢复实现另存 Runner 材料 |
| Runner claim、事件和结果写入已验证执行身份、Pod/Job UID，并在持久化边界校验 | [`runner_events.go`](../pkg/apiserver/jobs/runner_events.go)、[`service.go`](../pkg/apiserver/jobs/service.go) | 恢复须扩展现有 fencing，不能绕过 claim |
| Harbor 当前固定版本，环境配置关闭 SandboxClaim | [`runner.py`](../runners/harbor/runner.py)、[`requirements.txt`](../runners/harbor/requirements.txt) | 不引入预热池；是否支持恢复由固定版本源码与实验验证 |

[ACS 官方 Checkpoint 文档](https://help.aliyun.com/zh/cs/user-guide/clone-agent-sandbox-using-checkpoint) 当前限定：仅 ACS Agent Sandbox、仅文件系统；源 Pod 必须 Running 且 Ready；同一 Pod 同时只能有一个进行中的 Checkpoint；Running 后删除 CR 不能中断快照任务；克隆时 Pod spec 需与源保持一致。官方要求 `acs-virtual-node` 至少 v2.17.0。部署前重新核实地域、组件、CRD 版本和配额，以真实 ACS 验证为准。

因此不能等源环境丢失后才补做快照，也不能把文件系统一致性当成 Runner 与多个 trial 的应用一致性。首个门禁必须证明：固定版本 Harbor 能跳过已完成 trial、重连或重建恢复环境，并让所选 Agent 从持久化进度启动。若只能重跑未完成 trial，应如实记录恢复粒度，不能称为该 trial 的断点续跑；不满足用户目标时停止后续恢复实现，提交最小适配方案及影响供决策。

### 2.1 S2-1 本地能力实验

实验入口为 [`recovery_probe.py`](../runners/harbor/recovery_probe.py)，使用 Runner 固定的 `harbor==0.22.0`、`kubernetes==32.0.1`。从项目根目录运行：

```bash
python3.13 -m venv /tmp/eruun-harbor-stage2-venv
/tmp/eruun-harbor-stage2-venv/bin/python -m pip install -r runners/harbor/requirements.txt
/tmp/eruun-harbor-stage2-venv/bin/python runners/harbor/recovery_probe.py
/tmp/eruun-harbor-stage2-venv/bin/python -m unittest discover \
  -s runners/harbor -p 'test_recovery_probe.py' -v
```

探针退出码 `2` 表示实验成功建立了证据，但运行中 trial 的恢复门禁失败；`1` 表示依赖错误或实验失败，不能视为已验证。安装依赖需要网络，探针本身不创建 Sandbox、不执行 Agent、不读取 Kubernetes 配置或凭据，也不请求模型。测试依赖缺失或版本不符时明确失败，不通过 skip 隐藏。

实验用 Harbor 的真实 `Job.create` 恢复协调逻辑处理两个 trial：一个具有完整结果，另一个具有进度标记但没有完整结果。材料复制到新的临时根目录，重定位实验 fixture 中的绝对路径后删除源目录，再运行恢复协调。路径重定位仅是 fixture 准备，不是已实现的 Runner 持久化协议。

| 未完成 trial 的结果文件 | 已完成 trial | 未完成 trial | 原进度文件 |
| --- | --- | --- | --- |
| 不存在 | 跳过，结果摘要不变 | 使用新 trial 身份重新排队 | 原 trial 目录被删除 |
| 空文件 | 跳过，结果摘要不变 | 使用新 trial 身份重新排队 | 留在旧目录，但未载入 |
| 截断 JSON | 跳过，结果摘要不变 | 使用新 trial 身份重新排队 | 留在旧目录，但未载入 |

这是 **离线恢复协调实验**，不是实际 Runner 崩溃、模型调用或 ACS 恢复验收。固定包的行为说明原生 `harbor job resume` 不足以满足本阶段目标；不能据此开启生产恢复或将 S2-1 标记通过。

固定版本源码进一步确认以下适配边界：

| Agent | Harbor 声明原生 resume | 会话材料与缺口 |
| --- | --- | --- |
| Codex | 是 | 运行期间保存在 `/tmp/codex-home/sessions`，在 `finally` 中导出到 `/logs/agent/sessions`；导出错误可能被上游吞掉。恢复前必须独立验证材料，不能只检查进程退出 |
| Claude Code | 是 | 会话位于 `/logs/agent/sessions`；上游使用 `--continue`，需要绑定明确的源会话身份并处理后台任务的写入 |
| Terminus-2 | 否 | 需要另行实现和验证持久化恢复协议，不能由其他 Agent 的支持情况推导 |
| Oracle | 否 | 重跑解决脚本不等同于保存进度后继续，不能当作 LLM 恢复验证 |

证据来自官方 `harbor-0.22.0` wheel（SHA-256：`4c4c6571b3d160ed0cb45b82918136751fb08e7b8596412723ac00dde12eeabb`）及其中的 `harbor/job.py`、`trial/single_step.py`、`agents/installed/base.py`、`agents/installed/codex.py`、`agents/installed/claude_code.py`。`SingleStepTrial` 的 Agent 阶段不会调用 `resume`；原生恢复声明也不提供暂停写入屏障。

另外在本地模型协议 stub 上验证了真实 Codex CLI `0.154.0`、Claude Code `2.1.281` 的会话文件恢复。实验先完成第一轮对话，将第二轮模型响应挂起，**确认中断消息已写入完整的 native session 记录后**强杀进程，复制会话文件到新配置目录并删除原目录，再按原 session ID 恢复；检查已完成轮次标记、已保存的中断消息及新恢复消息。实验只返回文字、不执行工具；没有验证运行中命令、后台进程、Harbor Trial 生命周期或云文件系统快照。

负面实验发现：模型 stub 刚收到第二轮请求就立即强杀时，Claude Code 可能尚未将该用户消息写入会话文件，随后恢复会丢失该轮。早期在收到请求后等待 300 毫秒的实验曾通过，但延时不是持久化屏障。可复现脚本因此检查真实会话记录作为实验前提，不能将“请求已经发出”或“等待了一段时间”当成进度保存成功，更不承诺任意时刻强杀零进度丢失。

该实验保存在 [`native_resume_probe.py`](../runners/harbor/native_resume_probe.py)，在已安装上述两个 CLI 版本的 POSIX 系统运行：

```bash
python3 runners/harbor/native_resume_probe.py
python3 -m unittest discover -s runners/harbor -p 'test_native_resume_probe.py' -v
```

脚本从 `PATH` 查找 CLI，也可用 `--codex`、`--claude` 指定可执行文件；错版直接失败。它使用全新的临时配置和本机模型服务，未继承真实模型凭据；默认删除实验材料。需要保留证据时，传入尚不存在的 `--output-dir`，其中原始请求、日志、会话及摘要仅供本地检查，文件权限收紧为 `0600`。CLI 超时或失败时清理所属进程组。退出码 `0` 仅表示报告声明的会话恢复断言通过，不能代替本阶段完整门禁。

这个结论只覆盖上述两个 CLI 版本。Harbor 框架版本固定并不等于 Agent CLI 固定；恢复配置通过 `agentVersion` 固定上述版本，不能宣称其他 CLI 版本已经通过恢复验证。

### 2.2 实现顺序与待验证边界

首批目标包括 Codex、Claude Code；其他常用 Agent 按各自能力补充适配和测试，不能统一退化成未完成 trial 重跑。

最小适配在 Eruun 现有 Runner、环境适配和 jobs 模块内完成：保存未完成 trial 的原始配置、会话身份及采集进度；通过受控 Agent 生命周期建立跨 trial 写入屏障；新 Runner 直接恢复这些 trial 并调用对应 Agent 的原生恢复入口，避免上游重建 trial 或重新执行破坏性的 setup。仅发送 SIGINT、观察 CLI 退出或发现 JSONL 文件都不能独立证明完整屏障。

服务端继续复用阶段一的 claim、数据库锁、创建预算与 UID 清理。恢复必须建立新 execution key 和源执行关联，保留原绝对 deadline；现有健康执行的观察接管语义不变。结果源归档每执行唯一，不能把它直接改成可覆盖的 checkpoint 历史。ACS 克隆所需模板保留源 Sandbox 的原始 Pod 模板 spec；实际 Pod 的调度默认化与 runtime 注入兼容性等待云验收，不能直接使用重新计算 deadline 的普通创建模板。

真实 ACS 验收按维护者要求留待后续专门执行。快照/克隆、卷覆盖、旧执行隔离、凭据轮换、故障注入、RPO/RTO、屏障时长与万级背景压力都保持 **未验证**；不访问集群、不生成云资源，也不以本地测试替代这些证据。维护者已允许恢复时重放未确认完成的工具调用；实现要求 `replaySafe: true`。此授权不替代真实云验收，也不提供外部副作用的 exactly-once。

## 3. 权威状态与身份映射

下表描述归属和不变量，不新增第二套任务状态机。恢复点的数据库/对象存储形状在工作包 S2-1 冻结；优先复用现有持久化和制品边界，只有历史查询、容量或独立回收要求证明有必要时才新增实体。

| 对象/概念 | 权威来源与关联 | 不变量 |
| --- | --- | --- |
| 业务任务与空间 | `WorkflowQueue.TaskID/WorkspaceID` | 恢复仍属于原业务任务和空间；取消、绝对 deadline 及权限继续有效 |
| 执行记录 | `JobInfo`、`ExecutionKey`、`RunGeneration`、`Attempt` | 新恢复执行必须可与源执行区分；具体 key/attempt 推进规则在设计冻结，不盲目等同 Worker generation |
| Worker ownership | `WorkflowQueue.RunGeneration/RunToken/WorkerID` 与数据库租约 | 旧 Worker 不能推进状态或清理新资源；健康执行的观察接管保留现有语义 |
| Runner owner | 现有 claim 与 Job/Pod UID | 新 Runner 必须取得新执行的合法 claim；旧 owner 的事件、结果、快照提交全部失效 |
| trial 与 Sandbox | 第一阶段关联记录，加 namespace/name/UID | 名称只用于定位，UID 防止同名资源替换；恢复建立到新 Sandbox UID 的明确映射 |
| 恢复点 | Eruun 持久化的完整性及提交状态 | 关联源执行、参与 trial、进度版本、对象摘要、Sandbox/Pod/Checkpoint UID、供应商快照标识及模板依据；这些是设计要求，不是现有字段 |
| 快照是否可用 | Checkpoint CR 状态、供应商实际可用性及 Eruun 提交记录 | CR 成功只是必要条件；与完整 Runner 状态对应并提交后才可选择恢复 |
| 最终业务结果 | 现有结果保存与终态提交路径 | 快照成功不代表任务成功；新旧执行结果隔离，业务完成只提交一次 |

## 4. 一致恢复点协议

采用可调和、可重入的多步协议；数据库与云快照没有跨系统事务，不伪装成一次原子调用。

1. 当前合法执行在应用可恢复边界请求保存。冻结本次参与 trial 集合，暂停这些 trial 的进度推进与影响快照内容的写入；Harbor 和 Agent 将必要状态 flush 到已约定路径。若无法建立屏障，本次恢复点失败，不发布成功标记。
2. 生成不可变的 Runner 恢复材料：任务包/配置摘要、已完成 trial 与结果索引、未完成 trial 的逻辑进度、采集游标以及所需 Agent 状态。写入可被其他 Runner 读取的持久存储，验证摘要与格式版本。不得依赖原 `/work`、节点本地目录或即将过期的下载地址。
3. 在屏障内为每个需恢复的 Sandbox 提交快照，绑定当前 UID 与执行身份；同 Pod 串行、集群侧有界并发。记录提交意图和供应商返回结果，调用超时后先查已存在操作，避免盲目重建快照。
4. 等待全部必需快照成功且 Runner 材料可读，核验参与集合、版本、摘要和源身份后，在现有 ownership 事务保护下提交“该恢复点完整可用”的持久化标记。任一成员缺失不得混用不同轮次的 Runner 文件与快照。
5. 成功或明确失败后解除写入屏障。源执行可继续运行；之后的写入属于下一进度，不修改已发布材料。记录屏障时长和失败原因，并验证业务暂停预算。若源在屏障中故障，只能选此前已提交的恢复点。
6. 任一步控制面重启，依据持久化意图重建观察。已成功但未提交的云快照先核验再继续提交或回收；只有完整标记可驱动恢复。超时不能被当成供应商操作已停止。

首次实现以整个 Harbor Job 的一致恢复点为清晰边界。多个 trial 的集合大小直接影响屏障时间与快照成本；如果改为按 trial 独立恢复，必须先证明结果汇总、任务顺序及 Agent 状态可独立组合，并在 S2-1 冻结范围，不能实施中悄然改变保证。

## 5. 故障后的恢复、隔离与幂等

1. 重新校验空间权限、取消、总执行 deadline、恢复预算和恢复点可用性，选择最近的完整恢复点。无可用恢复点时明确失败或等待人工处理，不静默从头运行。
2. 在数据库事务中取得恢复资格并撤销原执行的有效写入资格；复用现有租约、claim 和条件写入。在同一业务任务上只允许一个获准恢复操作；重复请求或消息复用同一操作结果。
3. 隔离旧 Runner/Sandbox 后才允许新进程产生业务副作用。网络分区时仅“心跳过期”不能证明旧进程停止；必须确认终止或建立有效隔离。无法证明隔离时等待并暴露原因，不让两个 Agent 同时执行。数据库 fencing 只能防止 Eruun 写入，不能撤销已经发往第三方的动作。
4. 按第一阶段创建限速和并发准入克隆 Sandbox、启动新 Runner；对每个创建请求记录稳定的关联依据。请求超时、重复事件、Watch 重连和 leader 切换后先确认已有资源及 UID，不重复创建。
5. 新 Runner 验证材料及版本，获取自己的 claim，重新绑定新 Sandbox UID、地址和凭据，从保存进度启动；恢复后通过现有结果提交门禁。取消/超时在每次提交前重新判断，不能因恢复重置整项任务的时间预算。

旧执行迟到的 heartbeat、terminal、结果、快照提交和清理均拒绝。清理按 UID 及执行归属执行，禁止删除同名新实例；保留源执行与恢复执行的审计关联。

## 6. 生命周期与失败处理

- **保留与引用：** 分别制定完整恢复点、失败半成品和业务完成后的保留策略，限制总数量/字节/费用；TTL 不能提前删除仍被恢复操作引用的快照。保留最后可用点的期限、最大数量和空间配额在 S2-1 冻结，不能默认无限保留。
- **回收：** 先阻止新恢复引用，再等待活动引用释放并回收材料/云资源，最后提交删除状态；部分删除失败可重试且可观测。验证源 Pod/Job 删除的级联回收不会误删应保留的快照。Running 快照不能靠删 CR 中断，取消需继续追踪至终态后回收。
- **继续写入与外部副作用：** 恢复回退到已提交的逻辑进度；快照之后发生的外部调用可能已生效。可恢复任务必须定义可重放或业务幂等策略；不保证撤销外部写入。
- **存储边界：** 在目标 ACS 实测容器可写层、`EmptyDir`、共享卷和外部卷的覆盖范围，不推断 filesystem 包含所有挂载数据。未覆盖的数据必须独立持久化并纳入一致协议，否则拒绝该任务恢复。
- **凭据与网络：** 快照不得作为复制授权的手段；新实例使用重新验证的空间权限与有效凭据。IP、Pod UID、进程 PID、临时 URL 和连接均可变化；恢复材料不得把它们当稳定身份。验证凭据轮换、撤销、过期及快照中敏感文件的访问控制。
- **版本边界：** 记录任务包、Harbor/Agent、镜像及运行模板依据，核验克隆 spec 一致性。默认只保证已验证版本组合；不匹配时明确阻止恢复，不尝试静默迁移。

## 7. 实施子 PR 工作包

所有子 PR 更新本节验收状态和证据。实现字段、内部协议及必要公共 API 在对应子 PR 中明确，不在此预定路由。

| 工作包 | 依赖 | 改动层及交付 | 验收 | 回滚边界 |
| --- | --- | --- | --- | --- |
| S2-1 能力验证与契约冻结 | 第一阶段身份/按需 Sandbox 设计可用 | Harbor 固定版本源码与最小 ACS 实验；验证恢复粒度、Agent 进度、卷覆盖；冻结存储形状、版本、身份推进、保留和 SLO | 一次多 trial 故障恢复实验，完成 trial 不重跑，未完成 trial 按声明粒度继续；未通过不进入后续工作包 | 仅实验/设计，不改变当前执行 |
| S2-2 Runner 恢复材料与屏障 | S2-1 | `runners/harbor`、现有 jobs/制品层；持久化必要状态、版本与摘要、暂停/解除屏障 | 删除原 Runner `/work` 后另一 Runner 可验证材料；不完整/损坏材料拒绝；屏障失败能收敛 | 停止生成新恢复点，既有普通执行继续；保留可读材料 |
| S2-3 Checkpoint 调和与完整提交 | S2-2，第一阶段共享观察稳定 | 现有 jobs 与 K8s 适配/观察、最小 RBAC；多成员快照与完整标记 | 云调用超时、部分失败、重启、重复事件下不产生假完整点；同 Pod 无并行快照 | 停止新快照，追踪已运行任务至终态；不得直接删除运行中 CR 当作中止 |
| S2-4 恢复执行与 fencing | S2-3，第一阶段调度/准入契约稳定 | repository 事务、jobs、Runner、资源构建与清理；新身份、克隆、resume | 双 Worker/分区/重复恢复只有一个新执行获准；旧写入及误删拒绝；取消与 deadline 不被绕过 | 停止新恢复并排空活动恢复；回退版本前确认旧程序能读新持久化数据 |
| S2-5 保留回收与可观测性 | S2-3、S2-4 | 制品/清理路径、状态与指标、必要部署配置、操作文档 | 引用保护、过期/空间删除、半成品回收、凭据轮换均有证据；可解释当前停在哪一步 | 暂停破坏性回收，保留只读审计；不回滚删除数据 |
| S2-6 故障与容量验收 | S2-2 至 S2-5、第一阶段容量基线 | 测试工具、真实 ACK+ACS 报告、Current 使用文档与运行手册 | 下述矩阵及冻结 SLO 全部通过，重新验证 1 万并发下恢复压力 | 未通过保持 Draft；修正瓶颈后重复受影响实验 |

## 8. 验证矩阵与指标冻结

单元/集成测试覆盖事务和协议；Kind 或 fake client 只验证 K8s 协调，不能证明 ACS 能保存或克隆。真实云实验使用固定地域、组件、镜像、任务规模和数据量，记录原始指标及失败样本。

| 层次 | 必测场景 | 通过依据 |
| --- | --- | --- |
| 协议/存储 | 各步骤前后崩溃、对象缺失/损坏、重复提交、版本不匹配、某 trial 失败 | 从不发布不完整点；确定性重入或明确失败 |
| 事务/并发 | 双 Worker、旧 generation/claim、恢复与取消/清理竞争 | 单一获准执行，旧结果/清理全部拒绝；相关 Go 测试含 race |
| Runner/Harbor | 完成与运行中 trial 混合、采集游标、Agent 状态、新 `/work` | 结果一致，完成 trial 不重跑，恢复粒度符合 S2-1 证据 |
| 真实 ACS | Running/Ready 检查、同时快照、克隆 spec、卷覆盖、TTL、源删除 | 每项厂商约束均实测；新 UID/IP 下可恢复 |
| 故障注入 | Runner/节点丢失、API/DB/对象存储中断、Watch 断线、旧实例网络分区 | 恢复选择正确；不能隔离时不启动；依赖恢复后有界收敛 |
| 安全/清理 | 跨空间请求、凭据撤销/轮换、快照过期、引用与回收竞争 | 无跨空间读取、无过期授权复用、无引用中资源误删 |
| 规模/长时间 | 逐级增加快照/恢复并发，最终在 1 万运行 Job 背景下注入故障；持续多轮保存回收 | 第一阶段稳态 SLO 不退化，无无限积压、泄漏或快照风暴 |

S2-1 必须给出以下数值与测量口径，经小规模基线确定后写回本表，不能以“尽快恢复”替代。当前均为待冻结，非现有承诺：

| 指标 | 定义与需冻结参数 |
| --- | --- |
| RPO | 故障时距离最近完整恢复点的进度时间差；冻结 p95/p99 上限及快照周期，不能只统计快照发起时间 |
| RTO | 确认需要恢复到新 Agent 实际继续有效工作；冻结 p95/p99，单列故障检测、隔离、排队、克隆和装载耗时 |
| 业务暂停 | 每次一致性屏障的 p95/p99 与最大允许时长 |
| 成功率 | 快照和恢复分别统计有效尝试数、成功数、业务不支持/前提不满足数，不隐藏失败样本 |
| 并发及容量 | 快照/克隆启动率、进行中数量、每任务数据规模与 trial 数；总恢复压力计入第一阶段创建预算 |
| 成本与清理 | 每任务保留数量/字节/时间、半成品收敛时限、孤儿数量、积压最长等待时间 |

无重复获准执行、无越权恢复、无错误完整标记、无跨执行结果覆盖是正确性硬门禁，不以平均成功率抵消。

## 9. 阶段完成清单

- [ ] 第一阶段契约已纳入集成分支，既有健康任务接管语义未回归。
- [ ] Harbor/Agent 能力门禁通过，明确支持组合、恢复粒度与不支持边界。
- [ ] 存储、身份、屏障、保留、API 及量化 SLO 已冻结并有测试。
- [ ] Runner 材料与全部必需快照共同提交；完整恢复点在原 Runner 消失后仍可用。
- [ ] 新执行恢复与原执行隔离完成，旧事件/结果/清理不能污染当前执行。
- [ ] 真实 ACS 的快照、克隆、卷覆盖、凭据变化和故障注入报告通过。
- [ ] 万级背景下验证快照/恢复/回收压力，未破坏第一阶段稳定性标准。
- [ ] 运行手册解释失败、无恢复点、RPO/RTO、人工处置、回滚与费用控制；实际实现后才将相应文档标为 Current。
