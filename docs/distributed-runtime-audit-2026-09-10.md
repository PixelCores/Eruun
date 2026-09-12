# 分布式运行时审计与修复记录

> 状态：Historical / Audit。审计基线为 `main@3045955f75b332e2ac5f07e77c2dfa725f499eb9`。本文记录代码证据和在隔离环境中实际完成的验证，不构成生产高可用认证，也不承诺 exactly-once。

## 范围与原则

本次审查覆盖 API、Controller、Scheduler、Worker 四种运行角色，以及应用 Workflow、空间独立 Job、数据库执行租约、Redis/Kafka 消息、延迟任务、结果恢复、Leader 切换、schema 迁移和 Helm/Quickstart 部署。修复保留现有 `/api/v1`、JSON、v2 dispatch、数据库 ownership 和执行身份契约，没有引入新的业务实体、协调服务或兼容别名。

Redis/Kafka 消息确认保持 at-least-once 语义，数据库 ownership 和 fencing 拒绝旧 owner 的结果。外部副作用使用稳定幂等键容忍结果不确定时的重复执行，其严格幂等仍由对应适配器和目标系统保证。Workflow callback 保持现有单次投递语义；恢复只重放尚未持久化终态的 callback，不会自动重试已记录的失败。

## 已确认问题与处置

| ID | 失败证据与影响 | 修复 | 验证 |
| --- | --- | --- | --- |
| D01 | Kafka pending 以 correlation ID 为唯一键；同分区 `A/B/A` 会覆盖第一条 A 的物理位置，乱序确认可能越过未确认的 B 提交 offset。 | pending 改为按 partition/offset 保存独立物理记录，逻辑 ID 仅用于定位候选；只提交每个分区连续确认的最高 offset，提交失败回滚本地确认状态。 | 单元测试覆盖重复 ID、分区隔离、乱序 ACK、提交失败；真实 Kafka 覆盖乱序确认和 stale pending 重领。 |
| D02 | Redis 消费组被删除后，普通读取和 reclaim 返回 `NOGROUP`，原实现不能从已有 backlog 恢复。初次修复仍让启动时的 `EnsureGroup` 从流尾创建组，使预先存在的消息绕过 `NOGROUP` 恢复分支而被永久跳过。 | 所有缺失消费组统一从 `0` 创建；仅识别明确的 `NOGROUP` 后重建并有界重试，认证、网络和协议错误原样传播。 | 单元测试覆盖创建起点、错误分类和重试边界；真实 Redis 按“写入 backlog、启动时建组、读取”顺序验证初次启动及删组重启恢复。 |
| D03 | Redis `XAUTOCLAIM` 每次从 `0-0` 开始，较前的 pending 会使较后的消息长期饥饿。 | 按 stream/group 保存并推进服务端返回的 next cursor，扫描结束后回绕。 | 单元测试覆盖 cursor 推进、隔离和回绕；真实 Redis 构造不同 idle 时间的 pending 并验证后续页可达。 |
| D04 | delay/result dispatcher 结束任期时只停止循环，已交给 handler 但尚未 ACK 的消息仍标记为 in-flight，不能由继任者及时重领。 | 停止接收后等待本任期处理退出，并释放仍未完成的本地 in-flight 记录。 | race 测试覆盖取消期间的释放、重投和重复结果去重。 |
| D05 | Job 的部分提前终止路径忽略终态持久化错误，父 Workflow 可能正常结束，而数据库仍保留运行态。 | 具有分布式执行身份或 ownership 的 Job 终态写入错误向上传播；保存时继续校验 generation、token、owner，旧 attempt 不能覆盖新 owner。无执行身份的旧式本地调用保留既有 best-effort 行为。 | 故障注入覆盖正常、skipped、启动前取消、watcher 初始化失败、ownership 丢失和旧 attempt 结果拒绝；相关包 race 测试通过。 |
| D06 | 未被 Worker 认领的任务取消后没有执行 callback 的 owner；运行中取消若 Worker 在提交取消后崩溃，子 Job 和 callback 可能永久残留。 | 将取消 callback 状态持久化到现有 scheduling reason，增加有界分页的恢复循环；取消先终态化子 Job，再执行带唯一 execution key 的 callback；长 callback 续租，按执行身份和 UID 精确清理 Job/CronJob，身份不匹配的既有资源保持不动。 | 测试覆盖认领前取消、运行中取消、Worker 崩溃、callback 超时与终态记录前的恢复重放、已记录失败不重发、稳定 execution key、旧 callback owner、同名异身份 Job/CronJob、子 Job 收敛和 UID 删除。独立复审补充了缺失取消原因的回退。 |
| D07 | Controller 首次 informer cache sync 可无限等待，Leader 无法退出，后继任期也无法接管。 | 初次同步设置默认 30 秒的有界超时；超时返回错误并结束当前任期。 | 单元测试覆盖成功、超时和 context 取消；Leader 生命周期 race 测试通过。 |
| D08 | server 关闭未完整等待选举循环，可能先释放数据库 Lease；晚到的 `OnStartedLeading` 也可能重新启动已经失效的任期。 | 关闭顺序先撤销任期并等待选举/角色 goroutine，再释放 Lease；callback 在进入和返回边界检查任期 token。 | 测试覆盖晚到 start、leadership lost、关闭等待与 Lease 释放顺序；kind 中删除 Controller/Scheduler leader 后均由新 Pod 取得新 holder identity。 |
| D09 | readiness 只反映进程和部分外部依赖，数据库中断时仍可能接流量；将同一检查放入 liveness 会造成重启风暴。 | readiness 增加 2 秒超时的数据库时钟查询；liveness 保持进程存活语义。 | 单元测试覆盖成功、超时和查询失败；真实 MySQL 停止期间 readiness 失败、Pod 保持 Running 且 restart count 不变，恢复后重新 ready。 |
| D10 | 再次迁移开始前保留旧成功 marker；若迁移中途失败，validate 可能接受半完成 schema。GORM `HasTable/HasColumn` 还会把探测错误折叠成 false。 | 持锁后先将 marker 标记 incomplete，迁移成功才写 complete；使用显式、可传播错误的 table/column probe，并保留既有迁移锁。 | SQLite 故障注入和真实 MySQL 覆盖失败 marker、重试完成、探测错误及两个实例并发迁移（最大并发执行数为 1）。 |
| D11 | 空间独立评测 Job 的 Runner 上传先校验 Pod/Job 身份，再写入结果；两步之间父任务或执行 checkpoint 可能已被恢复流程终态化，旧 Pod 仍可发布。 | 授权时要求父任务正在运行，或由同一未过期租约执行取消收尾；结果事务内锁定并复查父任务、token 和 Job checkpoint，终态 checkpoint 一律拒绝。 | 单元测试覆盖传输期间父状态/checkpoint 改变、旧 Pod/旧 UID/旧 generation、取消期同一活跃 Pod 及过期租约。 |
| D12 | 应用级联删除与 Workflow 取消分散写入，进程失败可能留下可调度任务、未终态子 Job 或待发送 callback。 | 在已有事务和 CAS 契约中原子收敛应用/Workflow 状态，复用 durable callback marker 与子 Job 终态化路径。 | 故障注入覆盖并发状态改变、事务失败、callback 意图保留和未形成终态记录时的恢复重放。 |
| D13 | Controller 取消恢复需要读取并删除精确匹配的 CronJob，但部署 RBAC 缺少对应权限，恢复会永久停在 pending。 | Helm 与静态清单为 Controller 增加仅限 CronJob `get/delete` 的权限；删除仍受 execution identity 和 UID precondition 约束。 | 静态部署测试、Helm 渲染测试和取消恢复测试通过。 |
| D14 | Quickstart 每次生成新数据库/Redis 密码；已有 PVC 且 Secret 丢失时可创建不匹配凭据。Chart 也允许持久卷存在时修改 fullname、端口、数据库或密码，长 release 名还会产生超长依赖名。 | Quickstart 复用现存 Secret，并从旧 Chart StatefulSet 恢复缺失的 database 元数据；Chart 保留凭据 Secret 作为 PVC 身份 marker，lookup 现存 StatefulSet/PVC/Secret 并拒绝不兼容安装或升级；所有依赖名统一使用 63 字符 suffix-aware helper。 | Shell/Helm 测试覆盖旧 Secret、读取错误、PVC 检测、长名和端口；kind 覆盖 main Chart 升级、卸载保留数据后的 fullname 防漂移和四角色健康。 |
| D15 | 配置自定义 MySQL/Redis servicePort 只改变 Service 和探针，容器进程仍监听默认端口。 | 显式把端口传给 MySQL/Redis 进程并让 probe 使用同一端口。 | kind 分别以 MySQL `13306`、Redis `16379` 启动，Pod ready，进程级 `mysqladmin`/`redis-cli` 检查通过。 |

## 十轮连续审查记录

每轮都以前一轮完成修复后的工作树为输入。发现实质问题时先修复并完成聚焦验证，再进入下一轮；同一时点开展的并行检查不重复计数。独立审查者在后半程复核高风险状态转换和完整差异，主审逐项核验结论。

| 轮次 | 审查重点 | 结论、修复与该轮证据 |
| --- | --- | --- |
| 1 | 基线、权威状态与调用链 | 冻结 `3045955`，确认数据库任务状态是事实源，Redis/Kafka 是通知与投递层，Kubernetes Job 不是 execution ownership 的权威来源；形成 D01–D10 初始候选，排除已有防护覆盖的路径。 |
| 2 | Kafka 重复消息、乱序 ACK、提交失败、分区隔离 | 复现并确认 D01；改为物理 offset 记录和连续提交，补齐提交失败回滚及任期退出释放。单元与真实 Kafka 故障验收通过。 |
| 3 | Redis 消费组恢复、pending cursor、公平性 | 确认 D02、D03；只对 `NOGROUP` 重建，从 backlog 起点恢复，保存独立 group cursor。单元与真实 Redis 消费组丢失/多页 reclaim 通过。 |
| 4 | delay checkpoint、result outbox、任期切换 | 确认 D04；补齐 dispatcher 结束时 in-flight 释放，检查结果 outbox 仍使用逻辑消息 ID 去重且 ACK 发生在持久化之后。race 测试通过，没有改变外部副作用契约。 |
| 5 | Workflow/Job 终态持久化与 owner fencing | 确认 D05；消除忽略保存错误的正常返回，统一 generation/token/owner 条件写。故障注入验证失败可恢复且旧 owner 写入被拒绝。 |
| 6 | 取消、Worker 崩溃、callback 收敛 | 确认 D06、D12；增加 durable callback 恢复、子 Job 终态化、UID fencing、续租和有界 worker/page。独立复审发现 callback 恢复时空取消原因会丢失语义，修复为稳定回退原因后复核通过。 |
| 7 | 全局/空间准入与独立 Job 接管 | 确认 D11；在结果提交事务中复查父任务和 Job checkpoint，验证 cancelled admission 在清理完成前占用配额、独立 Job 接管、取消期结果收集及旧 attempt 拒绝。独立复审通过。 |
| 8 | Leader、informer、关闭顺序和 goroutine 生命周期 | 确认 D07、D08；为首次 sync 设上限，等待选举循环退出后释放 Lease，并 fence 晚到 callback。独立复审及 race 测试通过；kind 中实际删除双 Leader 后完成接管。 |
| 9 | readiness、迁移、Helm、安装升级和 RBAC | 确认 D09、D10、D13–D15。独立复审先后发现迁移 probe 隐藏错误、Controller 缺 CronJob 读删权限、持久凭据在 reinstall/长名/自定义端口下的缺口；逐项修复。最终复审确认不再修改不可变的 volumeClaimTemplate，main Chart 到当前 Chart 的真实升级通过。 |
| 10 | 最终差异、集成故障验收与 Go 简洁性 | 独立终审发现并修复四组相邻缺口：skipped/启动前取消/watcher 失败仍忽略身份化终态保存错误；同名旧 Job 遮蔽当前 CronJob 取消清理；Controller 缺 CronJob 精确删除权限；旧 Chart Secret 缺 database 元数据及卸载后 fullname 身份丢失。新增状态转换、Quickstart、Helm 和 kind 证据后重新审查，最终结论为 **CLEARED**，无未解决实质问题。 |
| 11 | PR 行级复审与 Redis 真实启动顺序 | 复审发现 D02 的初次修复被启动前 `EnsureGroup("$")` 绕过；改为统一从 backlog 起点建组，并用真实 Redis 覆盖预存 backlog 和删组后新实例启动。修复后聚焦复审无未解决实质问题。 |
| 12 | Callback pending 语义与恢复边界 | 先撤回“仅成功送达才能清除 pending”的过强结论，再核对基线代码和 Current 文档；确认 callback 保持单次投递，pending 表示尚未形成持久化终局，而不是尚未收到 `2xx`。收紧审计文档的 at-least-once 表述，并增加 HTTP 503 失败终态不自动重发的回归测试。 |

## 验证环境与证据

所有本地依赖使用临时 Compose 项目 `codex-eruun-audit-runtime`；集群场景使用 disposable kind 集群 `eruun-audit-20260910` 和显式 kubeconfig。测试只清理本次创建的资源。

已完成的真实依赖验证：

- MySQL repository integration race：并发执行租约、全局/空间准入、事务、callback、旧 owner fencing 通过。
- MySQL schema migration integration race：失败 marker、重试、table/column probe 和并发迁移锁通过。
- MySQL 数据库时钟 integration race：UTC、`+08:00`、`America/New_York` DST 场景通过。
- Redis Streams integration race：消费组丢失后 backlog 恢复及 AutoClaim cursor 公平性通过。
- Kafka integration race：重复逻辑 ID、乱序确认、offset 提交和 stale pending reclaim 通过。
- Job artifact MySQL integration race：并发结果发布与投递通过。
- Compose 本地依赖 smoke test 通过，没有把 skipped integration test 计为通过。

已完成的 kind/Helm 故障验收：

- 从基线 main Chart 安装后升级到当前 Chart，API、Controller、Scheduler、Worker 四角色健康，没有 StatefulSet immutable field 错误。
- Controller 与 Scheduler 各扩为两个副本，删除当前 Leader 后，新 Pod 取得不同的 holder identity。
- Worker 扩为两个副本，删除一个 Worker 后由 Deployment 恢复副本数。
- MySQL 中断期间 API readiness 失败而 liveness 不触发重启；恢复 MySQL 后 readiness 恢复。
- Redis 中断和恢复期间运行角色没有异常重启。
- Helm 卸载保留 PVC 后，缺失原 Secret 的同名重新安装被拒绝。
- 自定义 MySQL/Redis 端口实际监听、探针和 Service 一致。
- Quickstart 对真实集群执行 dry-run，并在第二次运行复用现存凭据。

最终冻结差异通过 Go 1.25.8 全量 `go test ./... -race -cover -count=1`、`go vet ./...`、`go build ./cmd/main.go`、`go fmt ./...` 和 `git diff --check`。敏感内容扫描、`go test ./deploy -race -count=1`、Quickstart 合约测试、Helm 渲染合约测试及最终容器构建均通过。终审新增 Go 修改后已重新执行全量 race；没有用早期结果替代最终代码验证。

## 保留边界

- Kafka/Redis 故障后的目标是避免越过未确认消息并允许安全重投；重复投递仍是允许且必须被幂等处理的结果。
- callback 恢复保证取消意图最终形成一个可审计的单次投递结果，不保证目标端成功处理，也不自动重试已持久化的失败。结果不确定时的重放复用同一 `Idempotency-Key`，但无法替外部 HTTP 服务提供事务性 exactly-once。
- kind 与本地真实依赖验证覆盖本次定义的故障触发，不代表所有生产网络分区、存储故障或多集群拓扑均已认证。
- 本次没有合并 PR、发布镜像或创建 Release。
