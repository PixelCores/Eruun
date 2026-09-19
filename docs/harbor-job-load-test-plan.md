# Harbor Job 模拟压测计划

> 状态：Draft / Proposal。这是一份待执行的压测计划，没有集群压测数据或已验证的容量上限。文中的接口和默认配置依据当前代码及 [空间 Job API](workspace-jobs-api.md)、[系统设置](system-setting.md)；阈值是本次实验的验收建议，不是产品保证。

## 目标与测量边界

在隔离的 Eruun 四角色部署中，向一个空间提交 **1000 个 `eval` Job**，每个 Job 包含一个 Harbor 0.22.0 `oracle` trial。使用 [可构建的合成任务镜像和批量提交器](../examples/agent-evaluation/load-test/README.md)：试验 Pod 同时启动五个休眠时长不同的线程，每个 Job 的最长线程由试验容器 hostname 确定为 **60–300 秒**，最大值为 Sleep 300。这是真实的 Eruun API、数据库、调度、Kubernetes Job/Pod、Harbor Runner、状态事件和结果保存链路；任务内容和等待时间是合成的，没有模型调用、模型凭据或真实评测工作量。只在客户端伪造请求响应无法测量后半段链路的上限。

分别回答三个问题：一次能可靠接收多少待执行 Job；在目标提交速率下，排队与提交延迟如何变化；每个空间及整个部署能稳定执行多少个并发 trial，以及制品保存何时追不上完成速率。报告只对记录的集群、角色副本、数据库、队列后端、镜像、资源配额和结果策略成立；不能从一次 1000 Job 实验推断其他配置或真实模型任务的上限。

`POST /api/v1/jobs` 返回 202 只表示已接受；`GET /api/v1/jobs/:taskID` 的任务终态、`collectionState=collected` 和 `deliveries` 的目标成功是三个不同的完成点。`options.concurrency=1` 是单个 Harbor Job 内 trial 并行度，不是 Eruun 全局调度并发。全局调度默认 `maxConcurrentJobs=100`、`maxConcurrentJobsPerWorkspace=10`；每个 Worker 的 `workflow-max-concurrent` 默认 100，还需考虑 Worker 副本、Kubernetes 配额及节点资源。调度策略允许设置的 10000 是配置校验边界，**不是实测容量**。

## 环境和负载准备

1. 使用专用集群、数据库/schema、Redis 或 Kafka、空间和对象存储。记录 Eruun 与 Runner 镜像 digest、Harbor 版本、节点数量及可分配 CPU/内存、Kubernetes API/etcd 与 CNI、MySQL 连接池及磁盘、队列后端、API/Controller/Scheduler/Worker 副本数和资源限制、空间 ResourceQuota，以及监控采样间隔。四个角色必须配置同一 Harbor Runner；先完成 schema 升级。隔离环境不应混入其他任务。
2. 按 [合成负载示例](../examples/agent-evaluation/load-test/README.md) 构建并推送 `task/environment/Dockerfile` 的镜像，最好在运行记录中固定 digest。把任务目录复制到临时位置，替换 `task.toml` 的占位 `docker_image`，打包并上传原生任务。`oracle` 参考解执行镜像内的线程休眠程序；verifier 检查五个线程均完成，并把每个 Job 的计划/实际时长写入 `load-timing.json` 制品。`[agent].timeout_sec=360`，外层评测预算仍需覆盖镜像拉取、Harbor 启动、verifier 和归档。先运行一个 Job，验证 reward 1、完整采集和保存成功，且制品报告的时长落在 60–300 秒；不满足则排查镜像和任务包后再开始压测。
3. 只上传 **一次** 原生 tar.gz 任务包，保存返回的 `data.id`，1000 个 Job 都复用该 `taskPackageId`。批量提交器使用 [合成请求模板](../examples/agent-evaluation/load-test/evaluation.json)：`traits.evaluation.agent=oracle`，不填写 model/credentials；`traits.evaluation.attempts=1`、`concurrency=1`；`traits.evaluation.timeoutSeconds=900`，给最长 300 秒的合成等待之外的准备和 verifier 留预算，Runner 另有 360 秒采集/停止余量。明确使用隔离数据库上的 `retentionDays=1`、`database/full`，让结果保存也接受压力；数据库完整副本不会因为源归档保留期结束而自动删除，实验结束后由隔离环境的清理流程处理。保持同一策略贯穿可比较的轮次。
4. 使用有 member 权限的空间 Bearer Token，提交时带 `X-Eruun-Workspace-ID`；修改 `workflow_scheduler` 需要系统管理员 Token。驱动程序不得把 Token、Runner 凭据或响应中的敏感信息写入日志。先读取并备份 `GET /api/v1/settings/workflow_scheduler` 的 `data.value`；只在专用环境通过 `PUT /api/v1/settings/workflow_scheduler` 的 `{ "value": { ... } }` 调整两个并发上限，每轮记录生效值，结束后恢复原值。降低上限不会终止已有 Job，必须排空后再切换轮次。

在开始 1000 个之前用提交器做 1、10、100 个 Job 的端到端预检；确认 trial Pod 可启动、最长 300 秒等待没有因超时提前终止、每个终态都有可信的 Runner terminal 和 Kubernetes 证据、结果归档完整、目标保存成功。预检还用于估算制品平均字节数、各 Job 的计划时长分布和每个并发 Job 的实际 CPU/内存占用。按资源请求做容量预算：默认资源 Trait 为 Runner **及每个任务环境**分别请求 1 CPU/2 GiB；在 Runner 和 trial 同时存活的阶段，`C` 个 Job 大致需要 `2C` CPU/`4C` GiB 的请求容量，还要给控制面、数据库和系统 Pod 留余量。比如 `C=100` 的预留量级是 200 CPU/400 GiB；节点不具备条件时不应把 Pending Pod 误报为 Eruun 调度上限，可先选更小的 C 或用明确记录的低资源 Trait 另做对照实验。

## 实验矩阵与执行步骤

使用 [批量提交器](../examples/agent-evaluation/load-test/submit.py) 按本轮速率发出请求。每轮生成唯一的 `runId`，请求 `name` 含 `runId` 和序号；名称并不提供幂等性。客户端记录每个序号的计划/实际开始时间、结束时间、HTTP 状态、响应 envelope 的 `code`、`data.taskId` 和错误类型，不能在网络超时后无条件重试创建：当前独立 Job 提交没有客户端幂等键。结果不明确时先通过运行记录和受限只读数据库核对，再决定是否补交；无法核对则将这一轮标记为不确定，不计入 1000 个已接受样本。任一轮在所有任务及保存目标排空后再切换配置，保持环境基线一致。若 `startedAt - scheduledAt` 持续增大，说明提交器自身已饱和；该轮不能当作目标 req/s 下的 Eruun 接收能力。

| 轮次 | Job 数 | 提交流量 | 单空间 / 全局并发上限 | 要回答的问题 |
| --- | ---: | --- | --- | --- |
| 基线 | 1、10、100 | 先串行，再 10 req/s | 10 / 100（默认） | 任务正确性、时间基线、采集/保存成本 |
| 排队 | 1000 | 20 req/s，约 50 秒发完 | 10 / 100 | 1000 条待执行记录能否可靠接受和排空；默认值下的排队尾延迟 |
| 提交突发 | 1000 | 逐级 50、100、200 req/s；每档独立一轮 | 10 / 100 | API/MySQL/队列接收速率和 p99 延迟的拐点 |
| 并发梯度 | 每档 1000 | 固定 20 req/s | 10、25、50、100 / 对应至少相同的全局上限 | Runner/Worker/调度/Pod/结果保存的稳定并发与吞吐；只在预检有足够资源时进下一档 |
| 聚合对照 | 每空间 500，共两个空间 | 合计 20 req/s | 每空间 10 / 全局 20 或更高 | 区分空间限制和整个 Eruun 部署的限制，检查公平性 |

全局上限若大于当前空间上限，单空间测试的实际 Eruun 准入仍受空间上限约束。并发梯度需同步记录 Worker 副本与每进程 `workflow-max-concurrent`，否则调高策略可能只增加队列积压。为了寻找实际上限，在一档完全排空且质量门槛通过后才增加一档；遇到失败、Pod Pending 或保存积压时固定其他变量，在最后通过档与首个失败档之间减小步长复测。基线和候选最高通过档至少重复两轮；报告中保留每轮结果及波动，不把偶然峰值当稳定上限。

每个合成 Job 的最长休眠在 60–300 秒之间。1000 个 Job 的纯休眠工作量在 `60000–300000` Job 秒之间；实际总量可从每份 `load-timing.json` 的 `plannedJobSeconds` 求和，不能直接按 300 秒乘以 1000。若 `C` 个并发且不考虑额外开销，完成时间下界为 `总计划休眠秒数 / C`；默认 `C=10` 时，仅休眠部分就可能约 1.7–8.3 小时。Pod 调度、Harbor 启动和保存让实际时间更长，且 1000 同时运行不属于默认配置。每轮的等待窗口应覆盖 `最大排队时间 + 900 秒执行预算 + 360 秒采集余量 + 保存排空时间`，不能只等五分钟就判为失败。

## 观测、计算和停止条件

驱动每 5–15 秒按已返回的 task ID 拉取 `GET /api/v1/jobs/:taskID`，并为每个任务保存各字段发生变化的时间及最后一次响应：业务 `status`、`executions`、`runnerStatus`、`collectionState`、`deliveries`；查询须限流并另记实际 QPS，避免 1000 个任务的同步轮询本身改变实验结果。API 记录 `POST /jobs` 的成功率、HTTP 4xx/5xx、p50/p95/p99、每秒 202 数；对任务分别记录提交 202 → 调度 queued/admitted、Kubernetes Job/Pod 创建和 Running、Runner phase=running、Runner terminal、任务终态、采集完整、保存目标成功的时间。`JobInfo` 对外 `executions` 含 `schedulingState`、`schedulingQueuedAt`、`start_time`、`end_time`，需要更精确的准入时间时在隔离数据库用只读快照和调度日志对照；轮询得到的转换时刻有一个采样间隔的不确定度。数据库快照仅提取与本轮 task ID 对应的必要列，不导出 `evaluation_info` 或 `internal_info`。

同时采集：等待/已准入/运行/终态任务数，实际 Running Runner Pod 与 trial Pod 数、Pending 原因、镜像拉取延迟、失败和 OOM；四角色 CPU/内存、重启、日志错误及 Worker lease 恢复；MySQL 活跃连接、锁等待、慢查询、CPU/IO、表大小；Redis/Kafka 消费积压和延迟；Kubernetes API/etcd 请求延迟、节点可分配资源；每个 Job 制品中的计划/实际休眠时长、原始归档的数量与字节、数据库完整副本增长、Controller 待保存/失败目标数和保存耗时。已配置 OpenTelemetry 导出器时补充 Runner 事件量、冲突数及接收延迟；没有导出器时不要假定这些指标可用。按 `runId` 存时间序列、匿名化任务清单、配置快照、错误分类和 dashboard 截图，保留 1 分钟以上粒度的趋势和每任务细节以便复核。

计算口径：`acceptance = 有 taskId 的 202 数 / 实际发出请求数`；`completion = 任务 completed 且 collectionState=collected 的数 / 已接受数`；`delivery = 全部选择的保存目标 succeeded 的数 / 已接受数`；`submit throughput = 成功 202 数 / 提交窗口秒数`；`drain throughput = 完整完成数 / 从首个执行到最后终态的秒数`。分别报告提交到 Runner running 的排队 p50/p95/p99、Runner running 到 terminal 的执行时长、terminal 到保存成功的 p95/p99，以及最后一个目标保存成功时的整轮排空时间。所有比例要列分子、分母；超时、取消、采集不完整和保存失败分别计数，不能只用 reward 或 HTTP 202 断言成功。

建议将某档标为“稳定通过”仅当两轮都满足：1000 个请求全部有明确 202/taskId、没有身份不明的重复创建；至少 99% 的任务可信完成并完整采集，剩余失败可逐项解释；全部目标最终成功保存且积压归零；没有持续增加的队列/保存积压、角色重启、OOM 或长时间 Pod Pending；提交 p99 与排队 p99 相比前一档的增长符合事先记录的业务 SLO。**99% 是实验门槛提议，不是 Eruun 的 SLA**；在执行前写定可接受的 p99 秒数、最大排空时间及容忍的失败率，不能在看到结果后改阈值。若关键依赖先饱和，则报告“在该依赖条件下的上限”及证据，而不是称为软件固有上限。出现集群/存储容量告警、持续 5xx、无法核对的重复提交或非预期任务执行时停止注入，保留证据并按 task ID 取消未完成 Job；确认 Kubernetes Pod 与保存任务排空后再清理隔离环境。

## 结果报告模板（执行后填写）

| 配置 / 轮次 | 接收 / 发出 | 完整完成 / 接收 | 保存成功 / 接收 | 提交 p99 | 排队 p99 | 峰值 Runner/trial Pod | 排空时间 | 首个瓶颈与证据 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 基线 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 |
| 默认上限、1000 Job | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 |
| 候选最高稳定档（两轮） | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 |
| 首个未通过档 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 | 待测 |

报告结论分别给出：在记录配置下的“单批可靠接受 Job 数”（至少测到 1000 时才能写 ≥1000）、“稳定提交速率”（req/s）、“稳定单空间与全局运行并发”（Job 数）、“稳定完成与保存吞吐”（Job/min），以及首个不通过档的原因、性能余量和适用范围。若只完成计划或预检，所有实测栏保持“待测”，不得填写推算值当成实测数据。
