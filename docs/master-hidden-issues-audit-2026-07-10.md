# Eruun `master` 潜在问题审计（2026-07-10）

> 历史认证条目已被统一账号和空间契约取代；当前行为见 [账号与空间](account-auth-workspaces.md)。


> 状态：Historical / Audit。本文记录对 `master` 的一次只读、证据化审计，不代表问题已经修复，也不替代各专题的 Current 行为文档。

## 审计基线与结论

| 项目 | 内容 |
| --- | --- |
| 审计日期 | 2026-07-10 |
| Git 基线 | `master` / `c987b8772cfa9cb906b6242fc587584ad7124e89` |
| 新发现 | 19 项：P1 14 项、P2 5 项；未确认 P0 |
| 交付边界 | 只记录问题、证据、建议和回归场景；不修改运行期行为 |

本报告优先收录当前 `master` 上能够由代码、既有测试或静态复现证明的问题。标记为“条件触发”的条目需要特定配置或输入，但危险路径已经存在；标记为“潜伏”的条目当前没有对外路由，恢复能力前必须处理。

### 严重级别

| 级别 | 定义 |
| --- | --- |
| P0 | 无需复杂前置条件即可造成集群/数据全面失陷或大面积不可恢复故障，需要立即阻断。 |
| P1 | 核心能力不可用、安全边界失效、持久化事实不一致、资源冲突或持续高负载，应优先修复。 |
| P2 | 需要特定条件才触发的正确性、恢复性、可用性或潜伏安全问题，应进入近期排期。 |
| P3 | 低风险的文档、元数据、测试或维护性偏差。 |

### 已有审计排除规则

- `docs/master-code-review-open-items.md` 中仍开放的 S-003、Q-007，以及 `docs/code-quality-audit-action-map.md` 已记录的治理项，不作为“新发现”重复计数。
- 默认清单中固定数据库/缓存凭据配合外部 Service 暴露的风险已在 `docs/tcp-ingress-nginx-dependencies.md` 中明确出现，本报告不重复计入 19 项；实际部署仍应轮换这些凭据并改为显式按需暴露。
- 纯粹的长文件、代码风格和架构重构建议不计入 Bug 清单。

## 问题总览

| ID | 级别 | 状态 | 问题 |
| --- | --- | --- | --- |
| D-001 | P1 | 已确认 | 默认 manifest 缺少 YAML 文档分隔符，`ClusterRoleBinding` 实际消失 |
| D-002 | P1 | 已确认（静态） | Helm Chart 的监听地址、探针和 RBAC 默认组合不可用 |
| D-003 | P2 | 已确认 | quickstart 默认端口与 Redis workload kind 和实际资源不一致 |
| A-001 | P1 | 已确认 | 启用或轮换 `apiAuth` 后旧策略最多继续生效 60 秒 |
| A-002 | P1 | 条件触发 | URL 校验与实际连接未绑定，存在 DNS rebinding 和代理 fail-open |
| A-004 | P1 | 已确认 | SSE 被 gzip 缓冲并受全局 30 秒写超时截断 |
| A-005 | P1 | 条件触发 | callback 自定义密钥 Header 可能随跨域重定向泄漏 |
| A-006 | P2 | 潜伏 | OAuth state 非原子消费，可并发重放且忽略删除失败 |
| A-007 | P2 | 已确认 | 大文件下载受总超时限制，失败后保留不完整目标文件 |
| W-001 | P1 | 已确认 | no-op 版本更新返回失败，但版本已经提交 |
| W-002 | P1 | 条件触发 | 并发手工执行可为同一 App 创建多个 active task |
| W-003 | P1 | 已确认（代码路径） | DelayDispatcher 收到第二条未来消息后可进入自唤醒忙循环 |
| W-004 | P1 | 已确认（多副本路径） | 三副本及以上时 `workflow-max-concurrent` 在执行节点失效 |
| W-005 | P2 | 已确认 | 单任务取消可覆盖 completed/failed/timeout 等历史终态 |
| K-001 | P1 | 已确认 | 模板中同一逻辑存储被拆成 VCT 与 standalone PVC |
| K-002 | P1 | 条件触发 | AdditionalObjects 使用空 GVK 去重，不同 Kind 同名对象互相吞掉 |
| K-003 | P1 | 已确认（差异路径） | `/version` 静默忽略 StatefulSet serviceName/VCT 变化 |
| K-004 | P2 | 已确认 | replicas=0 使用了“现存 Pod 全 Ready”的错误等待语义 |

## 部署与运维

### D-001 [P1][已确认] 默认 manifest 缺少 YAML 文档分隔符

- **现象与触发条件**：执行 `scripts/deploy-apiserver.sh` 或直接 apply `deploy/eruun-stack.yaml` 时，预期的 `ClusterRoleBinding` 不会成为独立对象。
- **影响**：`eruun-platform` ServiceAccount 没有预期集群权限，应用查询、资源创建、Pod exec 等核心能力会返回 RBAC forbidden；严格 YAML/Kubernetes 校验器也可能直接拒绝该文档。
- **直接原因**：`ClusterRoleBinding` 的 `roleRef` 后直接出现第二组 `apiVersion/kind/metadata`，中间缺少 `---`。YAML 解析后它变成一个带 `subjects/roleRef` 非法字段的 Secret，而不是两个对象。
- **深层成因 / 为何未发现**：Go 测试不解析部署清单；仓库没有把 YAML 多文档对象数量、Kind/Name 唯一性或 kubeconform 校验接入可重复质量入口。
- **相关代码**：[deploy/eruun-stack.yaml](../deploy/eruun-stack.yaml#L35-L61)、[scripts/deploy-apiserver.sh](../scripts/deploy-apiserver.sh#L38-L43)。
- **验证证据**：Ruby `YAML.load_stream` 解析得到 16 个文档，列表中没有任何 `ClusterRoleBinding`，第 5 个对象是同时带 `subjects/roleRef` 的 Secret。
- **推荐方案**：在 Secret 前补 `---`；增加 manifest 多文档解析、必需 Kind/Name 断言和 kubeconform 或无集群 schema 校验。
- **回归测试**：断言清单包含且仅包含一个 `ClusterRoleBinding/eruun-platform-cluster-admin`，Secret 不包含 RBAC 字段。

### D-002 [P1][已确认（静态）] Helm Chart 默认安装不可用

- **现象与触发条件**：使用 `deploy/helm/eruun` 安装后，API Pod 无法通过探针或无法管理 Kubernetes 资源。
- **影响**：Pod 可能持续 NotReady/重启；即使探针被人工放宽，默认 ServiceAccount 也没有 Eruun 所需的资源读写权限。
- **直接原因**：Chart 未设置 `ERUUN_BIND_ADDR`，服务沿用 `127.0.0.1:8000`；探针请求 `/readyz`、`/healthz`，实际路由位于 `/api/v1/*`；Deployment 没有 `serviceAccountName`，Chart 也没有对应 RBAC 模板。
- **深层成因 / 为何未发现**：Chart 与 raw manifest 分别维护，未共享端口、路由前缀和 RBAC 契约；没有 `helm template` 后的结构断言或安装 smoke test。
- **相关代码**：[Helm Runtime Deployments](../deploy/helm/eruun/templates/runtime-deployments.yaml)、[默认监听配置](../pkg/apiserver/config/config.go#L155-L219)、[API 前缀](../pkg/apiserver/interfaces/api/interfaces.go#L11-L16)、[health 路由](../pkg/apiserver/interfaces/api/health.go#L30-L36)。
- **验证证据**：静态渲染路径中不存在绑定地址环境变量、ServiceAccount 或 RBAC；路径与当前唯一 API prefix 不一致。本轮未运行 live Helm 安装。
- **推荐方案**：Chart 显式设置 `0.0.0.0:8000` 和 `/api/v1/healthz|readyz`；新增可配置 ServiceAccount、最小权限 ClusterRole/Binding，并用 `helm template` 校验。
- **回归测试**：渲染 Chart 后断言 Deployment 的绑定地址、探针路径、ServiceAccount 和 RBAC 引用一致；在 kind/k3d 中验证 Pod Ready 与最小资源操作。

### D-003 [P2][已确认] quickstart 默认值与实际资源不一致

- **现象与触发条件**：默认 manifest 模式下启用 port-forward 或等待 Redis 就绪。
- **影响**：提示和后台 port-forward 默认连接 Service 不存在的 8080 端口；Redis 实际是 StatefulSet，但脚本按 Deployment 查找后“资源不存在即跳过”，最终仍报告 readiness success。
- **直接原因**：`REMOTE_PORT=8080`，实际 Service 端口为 8000；`REDIS_WORKLOAD_KIND=deployment`，实际 manifest 使用 StatefulSet。
- **深层成因 / 为何未发现**：脚本默认值复制自旧端口/旧 workload 形态；`waitWorkloadReady` 把资源不存在作为 warning 后成功返回，没有把默认资源缺失视为配置错误。
- **相关代码**：[quickstart 默认值](../deploy/all_in_one_install_quickstart.sh#L30-L97)、[等待逻辑](../deploy/all_in_one_install_quickstart.sh#L438-L478)、[port-forward](../deploy/all_in_one_install_quickstart.sh#L480-L513)、[实际 Service/StatefulSet](../deploy/eruun-stack.yaml#L195-L228)。
- **验证证据**：脚本和 manifest 的固定默认值可直接静态比对。
- **推荐方案**：默认 remote port 改为 8000、Redis kind 改为 StatefulSet；默认资源不存在时 fail-fast，只有显式 optional 资源才允许跳过。
- **回归测试**：用 fake kubectl 记录脚本参数，断言等待 `statefulset/eruun-redis` 且 port-forward 为 `8080:8000`。


### A-001 [P1][已确认] `apiAuth` 更新存在最多 60 秒旧策略窗口

- **现象与触发条件**：通过 `PUT /api/v1/settings/apiAuth` 从默认 disabled 切换为 enabled，或轮换密钥、撤销角色/路由。
- **影响**：首次启用鉴权后请求仍可免认证最多一分钟；密钥和权限撤销也延迟生效，形成确定性的安全策略陈旧窗口。
- **直接原因**：middleware 内联创建 TTL 为一分钟的 `SystemSettingPolicyProvider`；更新服务只写 DB，provider 没有失效接口或版本检查。
- **深层成因 / 为何未发现**：测试只验证“第二次 Load 不查询仓库”，没有把系统设置写路径和 middleware 读缓存组成集成场景。
- **相关代码**：[middleware 装配](../pkg/apiserver/server.go#L480-L488)、[disabled 直接放行](../pkg/apiserver/interfaces/api/middleware/auth.go#L61-L88)、[provider 缓存](../pkg/apiserver/interfaces/api/auth/provider.go#L37-L89)、[设置更新](../pkg/apiserver/domain/service/systemsetting/system_setting.go#L142-L166)。
- **验证证据**：同一请求必须先经过 middleware，因而会缓存旧 disabled 策略；成功更新后没有任何代码能使该 provider 失效。
- **推荐方案**：安全设置使用 write-through/版本化缓存；单实例更新后立即失效，跨副本通过 Redis pub/sub、版本号或短轮询传播。无法实现前应将该安全策略 TTL 设为 0。
- **回归测试**：启用、密钥轮换、角色撤销后下一请求立即按新策略判定；多 provider 模拟跨副本失效。

### A-002 [P1][条件触发] 出站 URL 校验没有绑定实际连接

- **现象与触发条件**：攻击者控制 URL 域名 DNS，或服务配置了 `HTTP_PROXY/HTTPS_PROXY` 且本机无法解析目标域名。
- **影响**：`fileUrl` 下载、callback、远程 ConfigMap/Secret 等链路可能绕过“默认禁止私网”策略，访问集群内或云元数据地址。
- **直接原因**：校验阶段先解析并检查 IP，随后默认 Transport 再次解析并拨号；DNS 结果没有 pin 到实际连接。存在代理时，本机解析失败被直接视为允许。
- **深层成因 / 为何未发现**：现有测试覆盖了 URL 文本和第一次 DNS 结果，甚至锁定“存在代理时 unresolved host 放行”，但没有控制真实 DialContext 或代理端解析结果。
- **相关代码**：[URL 校验](../pkg/apiserver/utils/http.go#L122-L190)、[默认拨号](../pkg/apiserver/utils/http.go#L265-L297)、[convert 可达入口](../pkg/apiserver/domain/service/conversion/conversion.go#L80-L104)。
- **验证证据**：校验和 `client.Do` 是两次独立解析边界，代码中不存在已校验 IP 到 Transport 的绑定。
- **推荐方案**：解析一次后使用自定义 `DialContext` 连接已校验 IP，同时保留 Host/SNI；代理模式必须有独立显式策略和受信代理名单，不得因本机 DNS 失败而默认放行。
- **回归测试**：可控 resolver 第一次返回公网、Dial 阶段返回私网时必须阻断；代理解析到私网同样阻断。


- **验证证据**：仓库搜索仅发现 Agent 客户端设置 Authorization，Eruun API client 没有 token 字段或 header 写入。

### A-004 [P1][已确认] SSE 被 gzip 缓冲并在 30 秒后截断

- **现象与触发条件**：客户端请求组件日志时声明 `Accept-Encoding: gzip`，或日志/Shell SSE 持续超过 30 秒。
- **影响**：日志事件可能直到 gzip 缓冲满或请求结束才可见；长日志与 Shell 会被全局写 deadline 截断，不符合流式接口契约。
- **直接原因**：gzip skip 列表遗漏日志 SSE，`gzipWriter` 没有实现先刷新 gzip 再刷新底层 writer；HTTP Server 对所有路由统一设置 `WriteTimeout=30s`。
- **深层成因 / 为何未发现**：gzip 测试只在 handler 结束后解压完整 body；流式 handler 单测没有通过带 WriteTimeout 的真实 `http.Server` 验证分段到达和长连接。
- **相关代码**：[gzip middleware](../pkg/apiserver/interfaces/api/middleware/gzip.go#L12-L72)、[日志逐行 Flush](../pkg/apiserver/interfaces/api/component_logs.go#L44-L66)、[全局超时](../pkg/apiserver/server.go#L558-L568)。
- **验证证据**：日志 handler 的 `Flush()` 只会调用嵌入的底层 ResponseWriter，未调用 `gzip.Writer.Flush()`；net/http 的 WriteTimeout 是整个 response 的绝对 deadline。
- **推荐方案**：所有 `text/event-stream` 路由跳过 gzip；流式 handler 使用 `http.ResponseController` 清除或滚动 write deadline，非流式路由保留全局保护。
- **回归测试**：两阶段写入后，首事件必须在 handler 结束前可读；使用短 WriteTimeout 模拟持续 SSE，第二事件仍应成功。

### A-005 [P1][条件触发] callback 自定义密钥 Header 可被跨域重定向转发

- **现象与触发条件**：callback 配置 `X-Api-Key`、`X-Token` 等密钥 Header，合法目标返回 3xx 到另一个允许访问的公网域名。
- **影响**：合法 callback 服务存在 open redirect 或被接管时，平台凭据会泄漏给重定向目标。
- **直接原因**：callback 把配置的任意 Header 写入初始请求；`CheckRedirect` 只重新校验 URL，没有拒绝跨 origin 或剥离自定义敏感 Header。Go 默认只特殊处理少数内建敏感头。
- **深层成因 / 为何未发现**：测试关注日志脱敏和“重定向到私网被拦截”，没有验证跨域重定向后的实际 request headers。
- **相关代码**：[callback Header 与重定向](../pkg/apiserver/event/workflow/job/job_callback.go#L124-L161)、[现有重定向测试](../pkg/apiserver/event/workflow/job/job_callback_test.go#L177-L218)。
- **验证证据**：自定义 Header 保留由 net/http redirect policy 决定，当前 CheckRedirect 没有额外清理逻辑。
- **推荐方案**：默认拒绝跨 origin callback redirect；如需兼容，只允许显式安全 Header，所有凭据型 Header 跨 host 必须删除。
- **回归测试**：双测试服务器重定向，断言目标收不到 Authorization、Cookie、X-Api-Key 和用户配置的敏感别名。

### A-006 [P2][潜伏] OAuth state 不是原子消费

- **现象与触发条件**：恢复 OAuth 路由后，对同一 state 并发发送 callback，或 Redis Delete 失败。
- **影响**：多个请求可同时取得同一个 PKCE verifier；删除失败时 state 会保留到 TTL，但当前登录仍继续执行。
- **直接原因**：消费实现为独立 `Load` 后 `_ = Delete`；Redis 是 GET/DEL 两次命令，内存缓存也是两次独立加锁。
- **深层成因 / 为何未发现**：顺序测试只覆盖第二次读取不存在，没有并发 barrier 和 Delete 错误注入；当前路由未注册使该缺陷暂时不可达。
- **相关代码**：[OAuth state 消费](../pkg/apiserver/interfaces/api/auth/oauth_google.go#L231-L252)、[Redis cache](../pkg/apiserver/utils/cache/redis_cache.go#L59-L72)、[Delete](../pkg/apiserver/utils/cache/redis_cache.go#L113-L117)。
- **验证证据**：Load 与 Delete 之间存在明确竞态窗口，且删除错误被丢弃。
- **推荐方案**：为 cache 增加原子 `Consume/GetDel`；Redis 用 `GETDEL` 或 Lua，内存实现在同一锁内读取并删除；消费失败 fail-closed。
- **回归测试**：两个 goroutine 同时消费只允许一个成功；删除后端失败必须阻止换 token。

### A-007 [P2][已确认] 大文件下载会超时并遗留半文件

- **深层成因 / 为何未发现**：测试覆盖 API 错误和小文件成功，没有慢 body、中途断流、已有目标文件保护和原子性场景。
- **验证证据**：`os.Create`/`File::create` 发生在 copy 前，copy 错误直接返回，不执行 unlink/rename。
- **推荐方案**：下载只限制连接和响应头等待，不限制 body 总时长；同目录写临时文件，成功 close/fsync 后 rename，失败删除临时文件并保留已有目标。

## Workflow、事务与并发

### W-001 [P1][已确认] no-op 版本更新返回失败但版本已经提交

- **现象与触发条件**：`autoExec=true`、没有实际组件/资源变化、callback 非空，且 operation task 创建失败。
- **影响**：客户端收到 error 并可能重试，但 App 版本已经变化，没有 task/callback 审计记录；响应事实与持久化事实不一致。
- **直接原因**：该场景不走 `commitAutoExecVersionUpdate` 事务，而是先更新 App version，再创建 callback operation task；后者失败直接返回错误。
- **深层成因 / 为何未发现**：现有测试把该错误语义固化为预期，明确断言 `resp=nil/error` 同时版本从 `1.0.0` 变为 `1.1.0`，没有检查原子性契约。
- **相关代码**：[分支判定和提交顺序](../pkg/apiserver/domain/service/application/application_update_version.go#L159-L167)、[非事务更新](../pkg/apiserver/domain/service/application/application_update_version.go#L221-L265)、[task 创建失败](../pkg/apiserver/domain/service/application/application_update_version.go#L292-L307)、[锁定错误语义的测试](../pkg/apiserver/domain/service/application/application_version_autoexec_test.go#L988-L1044)。
- **验证证据**：既有测试稳定复现“返回失败且版本已提交”。
- **推荐方案**：把 App 版本更新与 operation task 创建放入同一 datastore transaction；任何失败都回滚版本。
- **回归测试**：task Create 返回错误时 response 为 error、版本仍为旧值、无 task、无 callback；重试可正常成功。

### W-002 [P1][条件触发] 并发手工执行绕过 App idle 约束

- **现象与触发条件**：两个实例或 goroutine 同时执行同一 App 的 workflow。
- **影响**：两个 active workflow 可同时修改、等待或清理同名 Kubernetes 资源，破坏“同一 App 同时只有一个工作流”的业务约束。
- **直接原因**：`EnsureAppWorkflowIdle` 与 queue task Create 是独立的先查后写；手工路径 idempotency key 为空，没有共同事务、per-App 分布式锁或 active-task 唯一约束。
- **深层成因 / 为何未发现**：schedule 路径已实现事务/idempotency，手工执行没有复用同一互斥边界；测试只覆盖串行第二次调用。
- **相关代码**：[手工执行](../pkg/apiserver/domain/service/workflow/workflow.go#L493-L508)、[task 创建](../pkg/apiserver/domain/service/workflow/workflow.go#L1443-L1460)、[无 idempotency 的 insert](../pkg/apiserver/domain/service/workflow/workflow.go#L1538-L1566)、[idle 查询](../pkg/apiserver/domain/service/workflow/workflow.go#L1607-L1647)。
- **验证证据**：检查与写入之间没有同步原语，两个请求可以同时观察到 idle 后分别 Add。
- **推荐方案**：使用统一的 per-App 分布式锁包住 idle 检查和 task insert，并在事务内完成；schedule/manual/version 共用同一互斥域。
- **回归测试**：双 goroutine barrier 让两个请求同时通过检查点，只允许一个创建成功，另一个返回 workflow running/409。

### W-003 [P1][已确认（代码路径）] DelayDispatcher 自唤醒忙循环

- **现象与触发条件**：调度器正在等待一个未来 item 时，收到第二条延迟消息。
- **影响**：leader 在下一个 timer 到期前持续执行 wake → requeue → notify 并反复排序，可长期占满一个 CPU 核。
- **直接原因**：wake 分支调用 `requeue(item)`；`requeue` 重新插入后又无条件向 wake channel 写 token，下一轮立即再次走 wake 分支。
- **深层成因 / 为何未发现**：测试验证排序和最终 dispatch，没有断言等待阶段应阻塞，也没有统计 `nextItem/requeue` 调用频率。
- **相关代码**：[scheduleLoop](../pkg/apiserver/event/workflow/job/delay_dispatcher.go#L211-L247)、[requeue 自通知](../pkg/apiserver/event/workflow/job/delay_dispatcher.go#L258-L294)。
- **验证证据**：第二条消息触发的 wake 被消费后，wake 分支自身立即制造下一枚 token，形成闭环。
- **推荐方案**：区分“新 item 打断等待时仅重新插入”和“失败重试需要通知”；前者不再 notify，或 drain/coalesce wake 后重算 timer。
- **回归测试**：加入两个未来 item，短观测窗口内循环必须阻塞，requeue 次数有界且 CPU 不出现 tight loop。

### W-004 [P1][已确认（多副本路径）] `workflow-max-concurrent` 在执行节点失效

- **现象与触发条件**：部署三个及以上副本，workflow subscriber 运行在 follower。
- **影响**：配置声明的并发上限无法保护 Kubernetes API、DB 和 Redis；一个 worker 批量读消息后可以启动超过上限的 controller。
- **直接原因**：limiter 只在 leader 调用 `Workflow.Start` 时初始化；多副本拓扑会在 leader 停 subscriber，follower 只调用 `StartWorker`，其 `workflowLimiter/taskGroup` 保持 nil，任务走 detached goroutine。
- **深层成因 / 为何未发现**：单实例测试同时拥有 Start 和 StartWorker 生命周期，没有覆盖 leader-dispatch/follower-execute 的三副本拓扑。
- **相关代码**：[limiter 初始化](../pkg/apiserver/event/workflow/workflow.go#L52-L69)、[nil limiter 执行](../pkg/apiserver/event/workflow/workflow.go#L250-L307)、[follower 启 worker](../pkg/apiserver/server.go#L715-L724)、[多副本角色切换](../pkg/apiserver/server.go#L986-L992)。
- **验证证据**：follower 路径没有调用任何 limiter 初始化函数，`StartWorker` 也不负责初始化。
- **推荐方案**：在实际 subscriber 生命周期初始化 limiter 和 task group；若配置语义是集群级上限，使用分布式 semaphore，否则明确并校验 per-worker 语义。
- **回归测试**：仅调用 follower `StartWorker`、max=1、投递两个阻塞任务，第二个不得并发启动；增加三副本拓扑测试。

### W-005 [P2][已确认] 单任务取消可覆盖历史终态

- **现象与触发条件**：对 completed、failed、timeout、reject 等历史 task 调用单任务取消，或取消与完成并发发生。
- **影响**：历史审计状态被改为 cancelled；controller 完成和取消互相 last-writer-wins，查询结果不可信。
- **直接原因**：单任务取消没有 active/terminal gate；非审批路径调用的 CAS 条件只有 `task_id=taskID`，等价于无条件字段更新。
- **深层成因 / 为何未发现**：`CancelAll` 已有 `shouldCancelAppTask` 并跳过终态，单任务路径没有复用；测试集中在 active/approval 场景。
- **相关代码**：[单任务取消入口](../pkg/apiserver/domain/service/workflow/workflow.go#L858-L875)、[无终态门禁的更新](../pkg/apiserver/domain/service/workflow/workflow.go#L1036-L1078)、[弱 CAS](../pkg/apiserver/domain/repository/workflow.go#L320-L332)。
- **验证证据**：任意 taskID 都能进入 status=cancelled 更新，CompareAndSwap 不包含原 status。
- **推荐方案**：复用 active 状态判定；以读取到的精确 status/approval checkpoint 做 CAS；controller 终态写也遵循合法状态迁移。
- **回归测试**：completed/failed/timeout/reject 取消必须保持原状态；complete-vs-cancel barrier 只能有一个合法迁移成功。

## Kubernetes 资源与 Traits

### K-001 [P1][已确认] 同一逻辑存储被拆成两个 PVC

- **现象与触发条件**：模板 StatefulSet 顶层使用 `tmpCreate=true,name=data`，nested init/sidecar 也声明同名 persistent storage，但默认 `tmpCreate=false`。
- **影响**：最终同时生成 `volumeClaimTemplates[data]` 和 `<appName>-data` standalone PVC；nested 容器挂载的不是主容器 VCT，可能导致重复 PVC、StorageClass 不一致和数据不共享。
- **直接原因**：顶层 tmpCreate 的 `data` 保持本地名称，递归 nested storage 走普通 persistent 重写并加 app 前缀；后续 processor 因名称不同无法复用 VCT。
- **深层成因 / 为何未发现**：模板重写按 trait 节点递归处理，没有在 main/init/sidecar 之间建立 logical storage identity；现有 clone 测试反而断言了两个不同名字，没有继续验证最终 Kubernetes 对象数量。
- **相关代码**：[顶层与 nested 重写](../pkg/apiserver/domain/service/application/application_template_clone_traits.go#L146-L179)、[递归 init/sidecar](../pkg/apiserver/domain/service/application/application_template_clone_traits.go#L262-L284)、[VCT/standalone 分叉](../pkg/apiserver/workflow/traits/storage.go#L61-L106)、[PVC 应用](../pkg/apiserver/workflow/traits/processor.go#L479-L533)。
- **验证证据**：现有 `TestCreateApplicationsFromTemplateKeepsTmpCreateStorageNamesLocal` 通过并锁定顶层 `data`、sidecar `<appName>-data` 的不一致。
- **推荐方案**：clone 前建立跨 main/init/sidecar 的 logical storage identity map；nested 同名引用继承顶层 name/tmpCreate/claimName/storageClass，只有显式不同 identity 才生成独立 PVC。
- **回归测试**：从模板 clone 一直执行到 StatefulSet 生成，断言只有一个 VCT、没有同逻辑 standalone PVC，所有容器挂载同一 volume。

### K-002 [P1][条件触发] AdditionalObjects 跨 Kind 误去重

- **现象与触发条件**：storage 与 RBAC 等 traits 生成 namespace/name 相同但 Kind 不同的 typed 对象，例如 PVC 与 ServiceAccount 同名，或 Role 与 RoleBinding 同名。
- **影响**：后出现的对象被静默丢弃；Pod 仍引用缺失的 ServiceAccount 时无法创建，RBAC 对象缺失时权限不可用。
- **直接原因**：去重 key 使用对象 GVK Kind；这些直接构造的 typed 对象没有设置 TypeMeta/GVK，Kind 为空，key 实际退化为 `/namespace/name`。
- **深层成因 / 为何未发现**：单元测试只覆盖不同名对象；代码假设 Go 类型会自动携带运行时 GVK，但直接构造的对象并不满足该假设。
- **相关代码**：[AdditionalObjects 去重](../pkg/apiserver/workflow/traits/processor.go#L258-L291)、[PVC 构造](../pkg/apiserver/workflow/traits/storage.go#L61-L102)、[RBAC 对象构造](../pkg/apiserver/workflow/traits/rbac.go#L69-L154)。
- **验证证据**：对象构造只设置 ObjectMeta，未设置 TypeMeta；`GetObjectKind().GroupVersionKind().Kind` 因而为空。
- **推荐方案**：使用实际 Go 类型或 Scheme/RESTMapping 得到 GroupKind；同 Kind/ns/name 且 spec 不同应 fail-fast，不应静默保留第一个。
- **回归测试**：构造同 ns/name 的 PVC、ServiceAccount、Role、RoleBinding，全部必须保留；同 Kind 冲突返回明确错误。

### K-003 [P1][已确认（差异路径）] StatefulSet 关键变化只更新 DB

- **现象与触发条件**：通过 `/version` 修改 store 组件的 headless Service identity、VCT size/storageClass 等 StatefulSet 不可变或半不可变字段。
- **影响**：DB 保存新 traits，workflow 可能返回 success，但集群 StatefulSet 仍绑定旧 Service/VCT；新建 Service 和旧 StatefulSet 可发生脱节，用户看到声明与运行态漂移。
- **直接原因**：版本更新完整覆盖 traits 并入库；`buildUpdatedStatefulSet` 只合并 replicas、updateStrategy、retentionPolicy 和 PodTemplate，保留 current 的 serviceName、selector、volumeClaimTemplates，比较函数也基于这个被裁剪的 updated 对象。
- **深层成因 / 为何未发现**：为了规避 Kubernetes immutable update，controller 选择静默保留旧值，但 API preflight 只保护 storage mode/VCT name，没有拒绝 size、storageClass 或 Service identity 变化。
- **相关代码**：[traits 完整覆盖](../pkg/apiserver/domain/service/application/application_update_version.go#L390-L405)、[PVC identity 校验范围](../pkg/apiserver/domain/service/application/application_update_version_pvc.go#L167-L182)、[StatefulSet 更新裁剪](../pkg/apiserver/event/workflow/job/job_statefulset.go#L361-L420)。
- **验证证据**：仅改变 VCT/serviceName 时，desired 与 current 的差异不会进入 updated spec，`statefulSetNeedsUpdate` 可返回 false。
- **推荐方案**：`/version` preflight 对需要迁移/重建的字段 fail-fast，并返回明确迁移提示；如确需支持，设计显式数据迁移/重建工作流，不能静默忽略。
- **回归测试**：修改 serviceName、selector、VCT name/size/storageClass 分别返回可识别错误，且 DB 不提交；允许字段仍正常更新。

### K-004 [P2][已确认] replicas=0 的 ready 判断语义错误

- **现象与触发条件**：`/version` 把 Deployment/StatefulSet replicas 更新为 0。
- **影响**：旧 Pod 仍 Ready 时 waiter 立即成功，尚未等待 Pod 消失；若 informer snapshot 已为空，则永远不 ready 直到超时。
- **直接原因**：DTO 和更新 contract 允许 0；job 把 desiredReplicas=0 传给 waiter；waiter 对 `desired<=0` 定义为“现存 Pod 全 Ready”，且 snapshot 不存在/总数为 0 时直接 false。
- **深层成因 / 为何未发现**：scale-to-zero 被复用到“等待 Pod Ready”的正副本逻辑，测试还锁定了这一分支，没有定义 stop/zero 的独立终态。
- **相关代码**：[replicas DTO](../pkg/apiserver/interfaces/api/dto/v1/types_version.go#L39-L67)、[更新应用](../pkg/apiserver/domain/service/application/application_update_version_contract.go#L266-L272)、[Deployment wait](../pkg/apiserver/event/workflow/job/job_deploy.go#L196-L220)、[StatefulSet wait](../pkg/apiserver/event/workflow/job/job_statefulset.go#L190-L214)、[waiter 判定](../pkg/apiserver/infrastructure/informer/waiter.go#L684-L692)。
- **验证证据**：desired=0 且 snapshot.total=ready=1 返回 true；total=0 在前置判断返回 false。
- **推荐方案**：若 `/version` 不支持停服，replicas<=0 fail-fast 并要求使用 stop API；若支持，单独等待 workload observed replicas=0 且匹配 Pod 数为 0。
- **回归测试**：1 个旧 Ready Pod 时不得立即完成；Pod 全部消失后成功；空初始 snapshot 可正确完成而不是超时。

## 系统性成因

这些问题跨越不同模块，但主要来自四类共同缺口：

2. **并发路径以串行测试为主**：先查后写、CAS 原状态、自唤醒 channel 和 leader/follower 生命周期缺少 barrier/topology 测试。
3. **流式能力复用普通 HTTP 默认值**：gzip、总超时、最终文件直写适用于短 JSON 请求，却不适用于 SSE 和大文件。

## 建议修复顺序

1. **立即修复部署阻断和安全边界**：D-001、D-002、A-001、A-002、A-003、A-004、A-005。
2. **收敛 Workflow 并发和事务事实源**：W-003、W-002、W-004、W-001、W-005。
3. **修复存储与 StatefulSet 运行态一致性**：K-001、K-002、K-003、K-004。
4. **补齐潜伏和运维体验问题**：D-003、A-006、A-007。

建议按以下小 PR 推进，避免把安全、并发、存储迁移和部署资产混在一个实现 PR 中：

- manifest/Helm/installer 契约与渲染校验；
- URL 实际连接绑定与 callback redirect Header 策略；
- SSE deadline/gzip 与原子下载；
- workflow manual exec 锁、DelayDispatcher wake 和多副本 limiter；
- version operation task 事务与 cancel 状态 CAS；
- template storage identity 与 AdditionalObjects GroupKind 去重；
- StatefulSet 不可变字段 preflight 与 scale-to-zero 终态。

## 验证记录与限制

在基线 `c987b877` 上已执行并通过：

```bash
go test ./...
go test -race ./...
go vet ./...
go build -o /tmp/eruun-server-master-c987b877 ./cmd
go mod verify
```

- Go 覆盖率命令 `go test ./... -coverprofile=...` 的总 statement coverage 为约 69.2%。
- manifest 额外通过 Ruby YAML stream 静态解析，确认没有独立 `ClusterRoleBinding`。
- `bash -n deploy/all_in_one_install_quickstart.sh` 与 `bash -n scripts/deploy-apiserver.sh` 通过；这只能证明 shell 语法正确，不能证明默认参数与资源契约正确。

本轮没有连接 live Kubernetes，也未执行真实 Helm/kubeconform 集成安装；本机未安装 `staticcheck`、`gocyclo`、`govulncheck`，因此没有把在线依赖漏洞结论写入本文。所有“已确认”问题均有当前源码、既有测试或确定性静态路径证据；需要外部条件的问题已明确标记为“条件触发”或“潜伏”。
