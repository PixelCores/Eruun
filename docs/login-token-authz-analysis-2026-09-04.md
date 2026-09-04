# 登录 Token 与授权机制评估

> 状态：Historical / Audit。基于 `main` 提交 `b3f3848aa5b24e851474b983a21692001337ce56` 的只读审计；“当前认证与授权链路”“缺口”等章节保留该基线事实。本文末尾记录同一 PR 后续实施的处置，不代表引入 JWT 或新的公开 API。

## 结论

当前项目已经具备常规的认证、会话和授权能力，并非“只有登录 Token、没有授权”：

- 登录后签发 256 bit CSPRNG 随机 opaque access token 和 refresh token；客户端只得到原值，数据库只保存 SHA-256 哈希。
- access token 通过 `Authorization: Bearer` 传递，15 分钟过期；refresh token 使用 `Secure`、`HttpOnly`、`SameSite=Lax` Cookie，服务器端会话绝对期限为 30 天。
- 每次请求从数据库重新读取用户、停用状态、`SecurityVersion` 和空间成员关系；登出撤销当前会话，改密、重置密码和停用账号撤销全部会话。
- 路由按公开账号接口、私有账号接口、业务接口和健康检查分类；未知业务路由默认拒绝。业务资源还在共享 datastore 边界按 workspace、namespace、AppID 和 taskID 校验归属。

因此，**当前阶段不建议仅因为 JWT 更常见就把本地 access token 改为 JWT**。JWT 是令牌表示格式，不等于授权机制。对于当前单一 Eruun API、要求权限变更和账号停用立即生效的模型，服务端 opaque session 更直接，且避免签名密钥轮换、声明过期、跨服务 audience 和撤销列表等额外复杂度。

该基线审计识别出的优先改进与 JWT 无关：refresh token 重放检测、空闲超时、过期会话清理和路由策略完整性自动检查。同一 PR 的后续提交已按下述处置实施，仍保留 opaque access token。

## 后续处置

- 每次成功 refresh 都把旧 refresh 的 SHA-256 哈希及其 Session 关系保留到会话 30 天绝对期限；任一旧代 refresh 被重放时，在事务内撤销该 Session 当前 access/refresh，并记录不含令牌值的安全事件。
- 同一个 refresh 的并发请求仍至多一个返回成功，但后续请求会触发重放响应，因此第一次返回的 token 也会失效；客户端必须保持 single-flight refresh。
- 增加 7 天 refresh idle timeout。成功 refresh 是活动信号，业务 API 请求不写 Session；30 天绝对期限不滑动。
- API 角色启动时立即清理绝对或 idle 过期 Session，之后每小时执行；已使用 refresh 哈希在对应绝对期限后清理。
- 增加基于实际 Gin 注册结果的完整性测试，要求每条路由恰好属于一个授权类别或显式健康检查例外。
- 未引入 JWT；原审计关于 JWT 适用边界的结论不变。

## 审计范围与事实边界

本次检查路径为：

`路由注册 -> Auth middleware -> access/refresh token 生命周期 -> User/Session 持久化 -> workspace 角色 -> scoped datastore -> 测试与 Current 文档`

检查了以下当前事实：

- 账号路由和业务路由：`pkg/apiserver/interfaces/api/*.go`
- 认证和路由授权：`pkg/apiserver/interfaces/api/middleware/auth.go`
- 会话签发、认证、刷新和撤销：`pkg/apiserver/domain/service/account/account.go`
- 密码、随机 token、验证码和限流：`pkg/apiserver/domain/service/account/credentials.go`
- 用户、身份、会话和空间模型：`pkg/apiserver/domain/model/accounts.go`
- 空间角色与资源边界：`pkg/apiserver/domain/service/account/workspaces.go`、`pkg/apiserver/security/access`
- 浏览器 Cookie、Origin 和 CORS：`pkg/apiserver/interfaces/api/accounts.go`、`pkg/apiserver/server_bootstrap.go`
- 当前接入契约：`docs/account-auth-workspaces.md`、`examples/account-auth-workspaces/README.md`

历史实现仅作为背景：`f828e89` 之前曾有 JWT/动态路由授权代码，但默认 `apiAuth.enabled=false`，OAuth 与授权管理路由未注册。该实现不是当前能力，且已被 `f828e89` 的本地账号、服务端会话和实时 workspace 授权替换，不应直接恢复。

Google 登录过程中确实会验证上游 OIDC `id_token`（JWT）的 RS256 签名、issuer、audience、有效期和 nonce；它只用于一次登录时确认 Google 身份，不是 Eruun 签发给客户端的本地 access token。

不在本次范围内：真实 GitHub/Google OAuth 联调、真实 SMTP/SMS、生产 ingress/TLS/APM 配置、渗透测试或迁移现有会话 token 格式。

## 当前认证与授权链路

### 1. 身份认证与会话签发

密码、邮箱/手机验证码以及 GitHub/Google OAuth 最终都进入本地账号模型。登录成功后：

1. `crypto/rand` 分别生成 32 byte access 和 refresh 随机值；Base64URL 无填充编码后长度为 43。
2. Session 只保存二者的 SHA-256 哈希，以及 access 过期时间、会话绝对过期时间、认证时间和用户 `SecurityVersion`。
3. access token 在 JSON 登录结果中返回；refresh token 只写入 Path 为 `/api/v1/auth` 的受保护 Cookie。

SHA-256 在这里不是低熵密码哈希：输入是不可预测的 256 bit 随机值，因此不需要使用慢密码哈希。密码本身使用带随机盐的 Argon2id。

### 2. 请求认证

中间件只接受标准 Bearer header 形态。每个非公开请求会：

1. 对 access token 做严格长度检查并计算哈希；
2. 通过唯一 `AccessHash` 查询 Session；
3. 读取 User；
4. 检查账号停用、`SecurityVersion`、15 分钟 access 期限和 30 天绝对会话期限。

服务端保存会话事实，因此不存在信任客户端角色或修改 JWT payload 的路径。数据库或认证依赖不可用时不会放行。

### 3. refresh 轮换与撤销

refresh 使用唯一哈希查询 Session，并在事务内锁定 User。CAS 以当前 refresh 哈希为条件，同时替换 access 和 refresh 哈希，因此并发使用同一个 refresh token 时只有一个请求成功，旧 access token 也立即失效。

当前撤销行为：

- 登出：删除当前 Session；
- 修改/重置密码：递增 `SecurityVersion` 并删除该用户全部 Session；
- 停用用户：递增 `SecurityVersion` 并删除该用户全部 Session；
- 空间移除或降权：不依赖 token 声明，下一次请求重新读取成员关系后立即生效。

### 4. 授权

授权不是由 token 格式提供，而由三层共同完成：

1. 路由矩阵规定公开、登录用户、system admin、member 或 viewer 最低权限；未知路由默认 `403`。
2. 每次业务请求按 `X-Eruun-Workspace-ID` 读取真实 WorkspaceMember，不能由客户端提交角色。
3. scoped datastore 在查询分页前加入 workspace 过滤，并在写入和 AppID/taskID 访问时再次校验归属。

本次静态清点得到 86 个已注册非测试 API 路由：82 个出现在公开/私有/业务策略表中，另 4 个是显式跳过认证的 health/readiness 路由，没有发现未分类后被意外放行的路由。

## 与通行安全建议的对照

| 检查项 | 当前实现 | 评估 |
| --- | --- | --- |
| token 不可预测性 | `crypto/rand` 生成 32 byte，即 256 bit 随机值 | 满足；高于 OWASP 对自定义 session ID 至少 128 bit CSPRNG 的建议 |
| token 内容 | 客户端 token 是无业务含义的 opaque 随机值 | 满足；用户、角色和权限保存在服务端 |
| 服务端存储 | 仅存 SHA-256 token 哈希 | 满足；数据库泄露不会直接得到 bearer 原值 |
| access 传输 | `Authorization: Bearer`，文档要求 HTTPS，不放 URL | 满足，生产入口到后端链路仍需部署验收 |
| access 有效期 | 15 分钟 | 满足短期 bearer token 原则 |
| refresh 存储 | `Secure`、`HttpOnly`、`SameSite=Lax`、窄 Path Cookie | 基线合理；认证写请求另做精确 Origin 校验 |
| refresh 轮换 | 每次刷新同时替换 access/refresh，CAS 保证单次成功 | 部分满足；缺少旧 token 重放后的会话族识别与撤销 |
| 即时撤销 | Session 服务端删除，`SecurityVersion` 与 User 每次读取 | 满足；也是当前方案优于无状态 JWT 的核心性质 |
| 动态授权 | workspace 成员关系每次读取，数据层二次隔离 | 满足当前“成员变更立即生效”需求 |
| 默认拒绝 | 未分类业务路由拒绝，缺依赖返回 503 | 满足；建议增加全路由策略完整性测试防止发布后才发现新路由不可用 |
| 日志与缓存 | 请求日志不记录 header、query 或 body；非健康接口 `Cache-Control: no-store` | 满足代码内基线；代理/APM 仍需部署验收 |
| 会话过期 | access 15 分钟、绝对期限 30 天 | 有绝对超时；没有显式空闲超时 |
| 过期数据清理 | 认证时拒绝过期 Session | 安全判断正确，但未发现定期删除过期 Session 的路径 |

## 需要优先评估的缺口

### P1：refresh token 重放后不能撤销当前会话

现有刷新先按 `RefreshHash` 找 Session，成功后把该 hash 覆盖为新值。第二次使用旧 token 时会得到 401，但旧 hash 已无法关联到原 Session。具体风险是：

1. 攻击者取得 refresh token；
2. 攻击者先于合法客户端完成刷新并获得新的 access/refresh；
3. 合法客户端使用旧 refresh 时失败；
4. 服务端无法从旧值定位 token family，攻击者的新 token 继续有效。

RFC 9700 对 OAuth 公共客户端要求 refresh token sender constraint 或带关系保留的轮换，以便发现重放后撤销当前活动 token。Eruun 的本地登录接口不因此自动成为 OAuth 授权服务器，这里的规范要求不是合规性断言；但同一 refresh token 盗用威胁成立，因此适合作为安全设计基线。按这个基线看，Eruun 已完成“轮换”，但尚未完成“关系保留与重放响应”。

下一步应先确定产品行为，再选最小数据结构：必须能把已经轮换掉的 refresh token 关联回会话族；命中重放时，原子撤销该会话当前 access/refresh，并记录不含 token 原值的安全事件。不要只增加另一个短期 JWT，因为它不能解决 refresh token 被盗后的恢复问题。

需要一起确定：并发刷新目前要求客户端串行化。启用 reuse detection 后，第二个并发请求应按安全事件撤销整条会话，这会把客户端并发 bug 变成重新登录；安全性更强，但前端必须继续保证 single-flight refresh。

### P2：没有会话空闲超时

Session 只有 30 天绝对期限，没有“连续一段时间未刷新即失效”的 idle timeout。access token 很短，但未使用多日的 refresh Cookie 仍可在绝对期限内恢复会话。

是否增加空闲超时取决于控制台敏感度和用户体验。可以以最近一次成功 refresh 为活动信号，避免每个业务请求都写数据库；具体期限应由产品风险而不是 JWT 格式决定。

### P2：过期 Session 未定期清理

过期 Session 会被认证逻辑拒绝，但代码中未发现按 `ExpiresAt` 删除历史 Session 的后台路径。长期运行会产生只增不减的数据。它首先是容量与运维问题，不是绕过认证的问题。

### P3：缺少路由注册与策略表的自动完整性断言

当前 86 个路由均被覆盖或显式跳过；未知路由也会安全地拒绝。但新增路由若忘记更新策略，测试不一定在合并前失败，生产会表现为该路由恒定 403。建议增加一个从实际 Gin 路由清单校验策略分类的测试，保证每个路由恰好属于一个分类。

## JWT 方案评估

### 方案 A：保留 opaque token 并加固（推荐）

适用前提：Eruun API 仍是唯一接受本地 access token 的资源服务；浏览器是主要客户端；权限、停用和会话撤销需要立即生效；尚无数据库读取性能证据表明认证查询是瓶颈。

优点：

- 延续当前数据模型和即时撤销语义；
- token 不携带用户或权限信息，泄露时不额外暴露 claims；
- 不新增签名私钥、JWKS、`kid` 轮换、issuer/audience 配置和验证分支；
- 可以直接解决 refresh 重放，而无需迁移 access token 格式。

代价：每次请求依赖共享数据库；水平扩容时要关注 Session/User/WorkspaceMember 查询延迟和可用性。当前授权本身仍需读取 workspace 与资源归属，所以单独换 JWT 不能消除数据库依赖。

### 方案 B：短期 JWT access token，授权仍实时查询

此方案可以省掉 Session/User 的部分读取，但 workspace 成员关系、资源归属以及立即停用/撤销仍要求服务端状态检查。若继续做 introspection 或 `SecurityVersion` 查询，JWT 的主要性能收益会被抵消，同时增加签名密钥与 claims 校验面。

在没有实测认证查询瓶颈前不推荐。若采用，JWT 至少需要固定允许算法、显式 `typ`、issuer、audience、subject、session ID、`iat/nbf/exp/jti`，使用非对称密钥和 `kid` 轮换；角色与 workspace 权限不应作为长期可信 claims。

### 方案 C：多资源服务离线验证 JWT

只有出现下列明确需求时再进入设计：

- 多个独立资源服务必须在不调用 Eruun Session 存储的情况下验证同一 access token；
- 对外提供第三方 API/SDK，需要标准化 issuer、audience、scope 和 JWKS；
- 接入外部 IdP，由明确的信任边界签发 access token；
- 压测证明数据库认证读取是主要瓶颈，并接受最长一个 JWT TTL 的撤销和权限陈旧窗口，或愿意维护 introspection/denylist。

这会改变公开认证契约、密钥运维、灾难恢复和撤销语义，应单独设计与迁移，不能作为本次加固的顺手修改。

## 原建议顺序与处置结果

1. 已确认重放响应为撤销整个 Session 并要求重新登录。
2. 已补重放检测、原子撤销、多代与并发刷新测试和安全日志；access token 保持 opaque。
3. 已采用统一 7 天 refresh idle timeout，并增加 API 角色每小时清理。
4. 已增加全路由授权策略完整性测试。
5. JWT 仍只在多资源服务、外部生态或实测性能需求成立时另行设计。

## 后续架构仍待确认的问题

1. 生产客户端是否只有 Eruun 自有 Web 控制台，还是已经有/近期会有第三方 CLI、SDK 或 API 客户？
2. 是否会有多个独立服务直接接受用户 access token，并要求离线验证？
3. 账号停用、成员移除和登出的目标撤销延迟是否必须接近即时，还是可接受 5–15 分钟窗口？
4. 当前或目标规模下，认证相关数据库查询的 QPS、p95/p99 和错误率是否已有数据？

重放响应与空闲期限已在本 PR 确认为“撤销整个 Session”及普通用户/system admin 统一 7 天。其余答案明确前，本审计仍建议不修改 token 格式。

## 验证证据

基线验证于 2026-09-04 执行：

- `go test -race ./pkg/apiserver/domain/service/account`：通过；
- `go test -race ./pkg/apiserver/interfaces/api/middleware`：通过；
- 静态路由清点：86 个注册路由 = 82 个策略路由 + 4 个显式 health/readiness 跳过路由。

后续修复验证于 2026-09-04 执行：

- `go vet ./...`：通过；
- `go test ./... -race -cover`：通过；
- `go test -race ./pkg/apiserver/domain/service/account ./pkg/apiserver/interfaces/api/middleware ./pkg/apiserver/domain/model ./pkg/apiserver`：通过；
- refresh 多代重放、并发重放、idle 边界、过期清理及真实路由授权完整性均有新增测试。

## 参考标准

- [OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)：opaque session ID 的熵、服务端状态、Cookie、超时和日志建议。
- [RFC 6750: Bearer Token Usage](https://www.rfc-editor.org/info/rfc6750/)：Bearer 是通用 HTTP 授权方案，token 可引用服务端授权信息；要求 TLS、短期 token，并避免 URL 传递。
- [RFC 9700: OAuth 2.0 Security Best Current Practice](https://www.rfc-editor.org/info/rfc9700/)：refresh token 轮换、关系保留、重放检测和撤销建议。
- [RFC 8725: JWT Best Current Practices](https://www.rfc-editor.org/info/rfc8725/)：若未来采用 JWT，需要承担的算法、类型、密钥和 claims 校验要求。
