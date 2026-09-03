# 账号、空间权限与接入

> 状态：Current。Eruun 后端新账号契约，按新建开发数据库验收。旧 JWT/动态路由授权配置及未启用的登录实现已移除；不迁移旧身份、空间或资源归属，也不自动清空现有数据库。

接入时先看 [逐步 API 示例](../examples/account-auth-workspaces/README.md)：包含可复制的 curl 请求、GitHub/Google 浏览器回调，以及团队邀请到资源访问的完整流程。本文作为接口、响应字段和部署要求的参考。

## 部署与配置

全部运行角色从 `--auth-config-file` / `ERUUN_AUTH_CONFIG_FILE` 读取同一份严格 JSON 配置，示例为 [`deploy/accounts.example.json`](../deploy/accounts.example.json)。复制到仓库外的私密文件，权限设为 `0600`，填写真实值后再部署。配置不通过系统设置 API 读取或修改，变更后滚动重启所有角色。未知字段、占位凭据、无效 Origin、缺少集群网络配置会使启动失败。MySQL 与 Redis 是必需依赖，Redis 同时承担验证码、OAuth state 和限流；Kafka 不能替代它。

| 配置 | 契约 |
| --- | --- |
| `origins` | 精确 HTTPS Origin 数组；没有通配、路径、尾斜杠。包含前端站点，如 `https://console.example.com` |
| `frontendURL` | 同一允许 Origin 下的邀请落地页，例如 `https://console.example.com/account` |
| `trustedProxyCIDRs` | 默认空，不信任转发头；使用入口代理时只填写可信代理网络，并限制 API 直连。入口应覆盖外部传入的转发头，否则 IP 限流不可信 |
| `bootstrapAdmin` | `email`、12–128 字符随机 `password`；只在空账号库创建。首次登录仅能读账号、改密、登出；必须换成不同密码，之后重新登录。重复启动不覆盖账号、密码或停用状态 |
| `github` / `google` | `enabled`、`clientId`、`clientSecret`、`redirectURI`；回调 URI 是前端页面，须在提供方控制台精确登记，且属于允许 Origin |
| `smtp` | `host`、`port`、`username`、`password`、`from`、`tls`；TLS 为 `implicit`（通常 465）或 `starttls`（通常 587），校验证书且无明文回退。禁用时整个对象留空 |
| `sms` | 阿里云 `accessKeyId`、`accessKeySecret`、已审核 `signName`、`templateCode`；模板参数名为 `code`，使用正式 SendSms HTTPS 接口。禁用时整个对象留空 |
| `workspace.clusterCIDRs` | 必填，准确列出 Pod、Service、节点及控制平面 CIDR，包括公网地址段（如有），以排除公共出口对集群网络的访问 |
| `workspace.storageClasses` | 可用存储类名单；空名单拒绝新建 PVC。省略 storageClass 时使用名单首项 |
| `workspace.ingressDomain` / `ingressClass` / `ingressNamespace` | 同时设置；不设置域名则拒绝 Ingress。生成域名为 `<组件名>.<空间 namespace>.<ingressDomain>`，需部署方提供 DNS 和证书 |
| `workspace.dnsNamespace` | 默认 `kube-system`；允许其中 `k8s-app=kube-dns` Pod 的 TCP/UDP 53 |
| `workspace.quota` | 覆盖统一空间配额；缺失条目补默认值：20 Pod，CPU requests 2 / limits 4，内存 requests 4Gi / limits 8Gi，10 PVC、20Gi 存储、GPU 0。NodePort/LoadBalancer 禁用 |

配置好后创建 Secret，再安装 Helm（MySQL/Redis 凭据按 [Helm 文档](helm-deployment.md) 提供）：

```sh
kubectl create namespace eruun-system
kubectl -n eruun-system create secret generic eruun-account-config \
  --from-file=accounts.json=/secure/eruun/accounts.json
helm upgrade --install eruun deploy/helm/eruun -n eruun-system \
  --set-string auth.existingSecret=eruun-account-config \
  -f /secure/eruun/values.yaml
```

安装脚本使用 `AUTH_CONFIG_FILE=/secure/eruun/accounts.json`，会在目标 namespace 创建同名 Secret；静态清单也引用该 Secret。Helm 拒绝空/占位 Secret 名或空 key；Secret 的内容由每个进程在启动时严格验证。schema migration Job 只迁移数据库，不引导账号、不连接 OAuth。

集群须启用 Pod Security Admission，支持 Restricted v1.34，以及真正执行 NetworkPolicy 的 CNI。本 PR 不安装 CNI 或容器运行时。服务端控制身份有建立空间安全基线、受限 ServiceAccount impersonation 的权限，以及用于空空间检查的资源清单读取权限；普通空间不能控制该身份。

## 登录及会话

统一 `/api/v1` 前缀、现有成功/错误封装。请求 JSON 不接受未知字段。`provider` 为 `email` 或 `phone`；邮箱去首尾空白并统一小写，不做 Gmail 去点等推测。手机号只接受中国大陆 11 位号或 `+86` 前缀，保存为 `+86`。密码原样处理、不 trim，12–128 个 Unicode 字符，Argon2id 加盐哈希。

| 方法 / 路径 | 请求及行为 |
| --- | --- |
| `GET /auth/methods` | 返回 password/email/phone/github/google 可用状态 |
| `POST /auth/codes` | `{purpose, provider, identifier}`；purpose 为 register/login/reset/bind，bind 需登录 |
| `POST /auth/register` | `{provider, identifier, code, password, name}`；已验证目标对应的新用户、身份、个人空间及成员在一个事务中创建；返回登录结果 |
| `POST /auth/login` | `{provider, identifier, password}` 或 `{provider, identifier, code}`，两者只能选一项 |
| `POST /auth/oauth2/:provider/start` | provider 为 github/google；`{link:false}` 返回 `authorizationURL` 并写入浏览器绑定 Cookie；link=true 需近期登录 |
| `POST /auth/oauth2/:provider/callback` | 前端提交 `{code,state}`，授权取消提交 `{error,state}`；要求发起浏览器 Cookie，一次性 state 消费后不可重放 |
| `POST /auth/refresh` | 空请求体，使用 refresh Cookie；返回新 access token 并轮换 Cookie，旧 refresh 和 access 立即失效 |
| `POST /auth/logout` | Bearer 登录，撤销当前会话并清 Cookie |
| `GET /auth/me` | 返回当前 user 及所有成员空间和角色 |
| `PUT /auth/password` | `{password}`；要求最近 5 分钟内的身份验证，撤销全部会话 |
| `POST /auth/password/reset` | `{provider,identifier,code,password}`，消费 reset 验证码并撤销全部会话 |
| `GET /auth/identities` | 返回当前用户登录身份；不会返回密码或令牌哈希 |
| `POST /auth/identities` | `{provider,identifier,code}`，消费 bind 验证码并要求近期身份验证 |
| `DELETE /auth/identities/:identityID` | 要求近期身份验证；不能解绑最后一种可用登录方式 |

验证码为随机六位数字，5 分钟有效，最多 5 次验证。Redis 原子执行尝试计数、一次性消费和限流：目标 60 秒间隔、10 次/小时，发送 IP 20 次/小时，认证 API IP 60 次/分钟，密码/验证码登录目标 10 次/15 分钟。发送失败明确返回上游错误并删除验证码，不报告已发送。不同用途不能混用验证码。

登录结果的 `data` 包含 `accessToken`、`tokenType: Bearer`、`expiresIn: 900`、`user`。随机令牌在数据库只保存 SHA-256 哈希；access 默认 15 分钟，会话最长 30 天，刷新不会延长绝对期限。refresh 位于 `__Secure-eruun-refresh` Cookie，Path `/api/v1/auth`，HttpOnly、Secure、SameSite=Lax。角色和停用状态每次请求从服务端读取，移除或降权影响后续请求。登出撤销当前会话，改密、重置或停用撤销全部会话。刷新不更新“近期身份验证”时间；敏感操作过期时重新登录。

GitHub/Google 使用授权码、PKCE S256、随机 state、浏览器绑定。Google 校验 RS256 签名、issuer、audience、有效期和 nonce；GitHub 每次重新读取稳定数字 ID，不以用户名标识账号。首次第三方登录创建个人空间，仅将提供方确认验证的邮箱绑定为邮箱身份。邮箱与现有账号重复时返回 33002，要求登录原账号再绑定；不会按同名邮箱静默合并账号。第三方凭据或错误正文不返回客户端。

## 浏览器接入

### 响应字段

账号与空间接口成功时均为 HTTP `200`，响应为 `{"code":0,"message":"","data":...}`，创建和删除也使用该封装。错误时根据 HTTP status 和业务 `code` 处理，不能只检查是否收到 JSON。

| 接口 | 成功响应的 `data` |
| --- | --- |
| `GET /auth/methods` | `{password,email,phone,github,google}`，均为布尔值；email/phone 表示验证码投递渠道是否配置，password 不表示当前用户已经设置密码 |
| 注册、登录、刷新、OAuth 登录回调 | `{accessToken,tokenType,expiresIn,user}`；`expiresIn` 单位为秒，refresh token 仅通过 Cookie 交付 |
| `GET /auth/me` | `{user,workspaces:[{workspace,role}]}` |
| `GET /auth/identities` | `[{id,provider,subject,...}]`；subject 为邮箱、`+86` 手机号、GitHub 数字 ID 字符串或 Google sub |
| `GET /workspaces` | `[{workspace,role}]`，不是直接的 Workspace 数组 |
| `GET /workspaces/:workspaceID` | `{workspace,role}` |
| 创建团队、接受邀请 | Workspace 对象，ID 取 `data.id` |
| `GET /workspaces/:workspaceID/members` | `[{id,workspaceId,userId,role,...}]`；成员操作路径取 `userId`，不是成员关系的 `id` |
| 创建/重发邀请 | `{id,workspaceId,email,role,expiresAt,...}`；不返回邀请 token，token 只在发给目标邮箱的链接中 |
| `GET /admin/users` | User 数组；使用 page/pageSize 翻页，不包含 total 字段 |
| 发验证码、改密/重置密码、绑定/解绑、登出、修改/删除团队、成员操作、转移、撤销邀请、停用用户 | `null`；OAuth 绑定成功回调也为 `null`，保留原会话，不返回新的登录结果 |

User 的业务字段为 `id/name/systemAdmin/disabled/mustChangePassword`；Workspace 为 `id/name/kind/ownerId/namespace`，kind 为 `personal` 或 `team`。模型响应还包含创建/更新时间。注意应用 DTO 使用 `workspaceID`，成员和邀请使用 `workspaceId`，所有者为 `ownerId`，转移请求为 `userId`，大小写必须按各接口契约填写。

### 会话与 OAuth

前端与 API 按同站 HTTPS 部署（例如 console.example.com / api.example.com）。跨源请求使用 `credentials: 'include'`。浏览器只把 access token 保存在内存，每次业务请求加 Bearer 和选中的空间 ID；页面刷新后通过 refresh Cookie 获取新 access token。不要把令牌放入 URL、localStorage、分析事件或客户端错误日志。服务端返回 `Cache-Control: no-store`；入口和 APM 也应禁用认证请求体及 Authorization/Cookie 的记录。

```js
const response = await fetch('https://api.example.com/api/v1/auth/login', {
  method: 'POST', credentials: 'include',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({provider: 'email', identifier, password})
});
const {data} = await response.json();
// 保存到内存；不写入浏览器持久存储。
const accessToken = data.accessToken;
await fetch('https://api.example.com/api/v1/applications', {
  headers: {Authorization: `Bearer ${accessToken}`, 'X-Eruun-Workspace-ID': workspaceID}
});
```

OAuth 前端先 POST start，再导航到 authorizationURL。回调页从当前 URL 读取 code/state（或 error），立即用 `history.replaceState` 移除查询参数，再向后端 callback POST；请求保留 Cookie，绑定身份时还保留当前 Bearer。后端只返回本地 access token，不把令牌重定向到 URL。刷新需串行化，同一旧 refresh 只有一个请求成功；刷新失败需重新登录。所有 `/auth/*` 非 GET 请求必须有配置允许的 Origin，CLI 也需显式发送 Origin。

调用公开登录/刷新接口时省略 Authorization，避免已过期的 Bearer 先被中间件拒绝。OAuth 绑定要求 start 与 callback 使用同一个本地会话；整页跳转后可先通过 refresh 恢复该会话的 access token，再提交 callback。刷新保持会话 ID 和原始认证时间，不能代替近期登录；若中途重新登录或超过 5 分钟，应重新发起绑定流程。

邀请落地页读取 `#invitation=...` 后清除 fragment，注册/登录并验证目标邮箱，最后提交接受接口；fragment 不发送到 HTTP 服务器日志。邀请链接也是凭据，应只在内存保存。

## 空间与权限

使用用户、身份、会话、空间、成员、邀请六类模型；个人和团队共用空间模型。用户、空间是稳定随机 ID，namespace 为 `eruun-ws-<无连字符空间UUID>`，不包含邮箱和手机号，创建后不可修改。应用只存一份必填 `workspaceID`，组件、工作流、任务通过所属 AppID 确定空间。系统管理员管理用户及集群配置；访问具体空间仍需成员资格。

| 角色 | 资源 | 成员管理 |
| --- | --- | --- |
| owner | 完整资源操作 | 邀请/管理 admin/member/viewer、转移归属、删除空团队 |
| admin | 完整资源操作 | 邀请/管理 member/viewer，不能管理 owner/admin |
| member | 创建、部署、生命周期、配置、日志、Shell、文件 | 可退出团队 |
| viewer | 应用列表元信息、应用及组件运行状态 | 可退出团队；无配置、Secret、日志、文件、模板、Shell 权限 |

只读列表使用显式 `id/name/namespace/workspaceID/version` 白名单，状态接口移除异常详情文本。工作流与组件完整响应只向 member 及以上开放。每个空间恰有一个 owner，`Workspace.OwnerID` 是唯一所有权来源，转移在事务内完成，原 owner 变为 admin。owner 不能直接退出；个人空间不邀请、不转移、不删除。

| 方法 / 路径 | 请求 |
| --- | --- |
| `GET /workspaces`、`GET /workspaces/:workspaceID` | 成员空间及有效角色 |
| `POST /workspaces`、`PATCH /workspaces/:workspaceID` | `{name}`；创建团队/修改团队名 |
| `DELETE /workspaces/:workspaceID` | owner 近期登录；无应用、无运行任务、无残留业务 Kubernetes 资源才能删除 |
| `GET /workspaces/:workspaceID/members` | 成员列表与有效角色 |
| `PATCH /workspaces/:workspaceID/members/:userID` | `{role: "admin" | "member" | "viewer"}` |
| `DELETE /workspaces/:workspaceID/members/:userID` | 移除成员或本人退出 |
| `POST /workspaces/:workspaceID/transfer` | `{userId}`；owner 近期登录，目标为未停用的已有成员 |
| `POST /workspaces/:workspaceID/invitations` | `{email,role}`；真实邮件发送，7 天有效；重发使旧值失效 |
| `DELETE /workspaces/:workspaceID/invitations/:invitationID` | 撤销邀请 |
| `POST /workspace-invitations/accept` | `{token}`；当前用户须拥有已验证目标邮箱，接受一次，同一成员重复接受幂等 |
| `GET /admin/users?page=1&pageSize=20` | 系统管理员；pageSize 最大 100 |
| `PATCH /admin/users/:userID` | `{disabled:true|false}`；不能停用系统管理员，停用撤销所有会话 |

业务请求通过 `X-Eruun-Workspace-ID` 选空间，省略用个人空间。客户端 namespace 与空间不一致直接拒绝。应用和任务 ID、批量查询、模板引用都校验归属；数据库查询先加空间过滤再分页，应用列表缓存按空间分开。权限不来自客户端角色或令牌声明。系统设置、集群导入和编程语言写入仅系统管理员可操作；导入目标也须属于选中空间。未明确加入权限表的新路由默认拒绝。

注册、团队创建、保存应用只写数据库。首次部署在任何工作负载写入前完成 namespace、安全身份、Pod Security Restricted、NetworkPolicy、ResourceQuota 和 LimitRange。已有同名 namespace 的 `eruun.io/workspace-id` 不一致会失败；并发创建重新读取验证归属。初始化部分失败可幂等重试，不删除已有资源，也不开始工作负载。

API 资源操作及后台执行使用 namespace 内受限 `eruun-runner` 身份；工作负载本身使用无 token 挂载的 default ServiceAccount。Pod 必须非 root、禁止提权、drop ALL capabilities、RuntimeDefault seccomp。镜像须支持非 root USER，或在 securityPolicy 设置非零 runAsUser。入口及展开后的最终容器（含 init、sidecar）均校验；拒绝 host namespace、hostPath、hostPort、任意 ServiceAccount、额外 RBAC、CloudJob、跨 namespace、NodePort/LoadBalancer、任意 Ingress 注解/域名/默认后端、不允许的存储类。

展开后的任务在资源比较和持久化之前写入与请求边界相同的安全默认值；规范化会完整替换对象内容，包括清除已移除的嵌套字段（如 Localhost seccomp 的 localhostProfile），相同配置再次部署不会因这些默认值触发 Deployment/StatefulSet 滚动更新。延迟 Job 到期时从已提交 JobInfo 的 AppID 读取应用及空间，重新验证 namespace 归属，并使用该空间的受限客户端执行。Controller RBAC 仅增加 namespace 读取和指定 `eruun-runner` 的 impersonation，不负责空间初始化。

延迟通知须与持久化检查点一致，不一致的通知仅确认消息，不修改检查点，正确任务仍可由数据库恢复。对已核实检查点的空间/工作负载校验失败，先按执行身份和当前状态原子写入失败终态，再确认消息；未创建的 Job 不标为已派发。失败写入或消息确认失败可重试，已有终态、新一代执行及已交给结果处理的任务不会被覆盖。

网络默认仅允许同空间通信、DNS、公共 HTTP/HTTPS 出口和配置的入口控制器；排除其他私网及 `clusterCIDRs`。配额和 LimitRange 由部署配置统一控制。Kubernetes 原生准入与 NetworkPolicy 是实际隔离的一部分，需要真实集群验证其生效。后台任务从持久化应用加载空间，不使用用户 access token；请求退出不影响已排队任务归属。

## 错误与验证证据

沿用 [统一错误契约](api-error-response-contract.md)。401 表示登录/令牌无效，403 为权限不足；33000 输入错误，33001 身份/空间冲突，33002 先登录原账号绑定，33003 验证码无效/过期，33004 限流（429），33005 需近期登录（403），33006 首次改密（403），33007 发送失败（502），33008 空间非空（409）。其他上游失败返回统一 502，内部错误脱敏。

自动化证据覆盖 SQLite 事务与唯一索引、Redis Lua 消费/并发/过期/限流、两提供方的 HTTP/JWKS 模拟、团队角色及邀请、HTTP 认证与空间过滤、Kubernetes fake client 基线和最终请求校验、延迟任务跨空间拒绝及受限身份、Deployment/StatefulSet 重复部署幂等性、部署模板与安装脚本。SMTP/阿里云通过适配器测试验证协议及失败，不消耗真实消息额度。

真实 GitHub/Google 应用、SMTP/阿里云账号和 Kubernetes 集群由部署环境提供，本地模拟不能代替真实联调。部署验收应分别记录：两提供方真实回调、邮件/短信实际投递、两个空间的网络互拒及公共出口、SA impersonation/Pod Security 拒绝、配额、并发首次部署、删除空 namespace。配置 Secret 不纳入 PR，不自动操作现有开发数据库。

本次自动化验收（2026-09-03，macOS arm64，Go 1.25.8）：Go 格式检查、`go vet ./...`、`go test ./... -race -cover`、服务端构建、安装脚本测试、Helm v3.19.0 模板测试及敏感内容检查通过。MySQL 的行锁 SQL 和二进制身份列类型另有契约测试；实际 MySQL 并发和真实集群仍按上面的部署验收清单执行。

`go-licenses/v2@v2.0.1 report ./cmd` 输出 138 条许可证记录。新增认证运行依赖为 Apache-2.0/BSD-3-Clause；测试使用的 GORM SQLite 驱动为 MIT，SQLite Go 包声明 MIT（内嵌 SQLite 为 public domain）。原有 `endpoint-util v1.1.0`、`nas-20170626/v2 v2.0.2`、`tea-utils v1.4.3`、`tea-xml v1.1.2` 的模块分发包缺少独立 LICENSE，扫描器标为 Unknown，人工核对这四个版本的 README 均声明 Apache-2.0。扫描还提示汇编文件无法继续分析依赖。上述是技术尽调记录，不替代分发时的许可证文件整理或法律审查。

参考：[OWASP 密码存储](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)、[GitHub OAuth](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps)、[Google OIDC](https://developers.google.com/identity/openid-connect/openid-connect)、[阿里云 SendSms](https://help.aliyun.com/zh/sms/developer-reference/api-dysmsapi-2017-05-25-sendsms)、[Kubernetes 多租户](https://kubernetes.io/docs/concepts/security/multi-tenancy/)。
