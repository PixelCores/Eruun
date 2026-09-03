# 账号与团队 API 示例

> 状态：Current。对应 [账号与空间 API 文档](../../docs/account-auth-workspaces.md)。示例中的域名、邮箱、手机号、密码、验证码和 ID 均为占位值，需要换成自己的测试环境数据；没有固定测试账号或通用验证码。

## 1. 准备请求环境

使用 Bash、curl（支持 `--fail-with-body`）和 jq。API 与前端须使用同站 HTTPS；`ORIGIN` 必须与部署配置中的允许来源完全一致。先完成 [部署配置](../../docs/account-auth-workspaces.md#部署与配置)。

以下命令在同一个 Bash 会话中执行。Cookie 文件位于私有临时目录，退出时删除；access token 只保存在当前 shell 变量中。不要开启 `set -x`、`curl -v` 或把含真实密码的命令保存到共享脚本/终端记录。

```bash
API='https://api.example.com/api/v1'
ORIGIN='https://console.example.com'
umask 077
ACCOUNT_TMP=$(mktemp -d)
COOKIE_JAR="$ACCOUNT_TMP/cookies.txt"
trap 'rm -rf "$ACCOUNT_TMP"' EXIT

api() {
  local api_path="$1"
  shift
  curl --silent --show-error --fail-with-body \
    --cookie "$COOKIE_JAR" --cookie-jar "$COOKIE_JAR" \
    --header "Origin: $ORIGIN" --header 'Content-Type: application/json' \
    "$API$api_path" "$@"
}
auth_api() {
  api "$@" --header "Authorization: Bearer $ACCESS_TOKEN"
}

api '/auth/methods'
```

`data.email/phone/github/google` 是可用方式开关。未配置 SMTP/短信时不要展示相应验证码入口。下面的写请求是独立操作示例，按需要选择执行；先检查响应 `code=0`，再使用 `data`。

## 2. 邮箱注册与手机号注册

先发送注册验证码，成功响应为 `{"code":0,"message":"","data":null}`：

```bash
api '/auth/codes' --data-binary @- <<'JSON'
{"purpose":"register","provider":"email","identifier":"developer@example.com"}
JSON
```

收到邮件后，将下方 code 换成实际六位验证码，将 password 换成自己的 12–128 字符密码。成功即登录，并自动创建个人空间：

```bash
LOGIN=$(api '/auth/register' --data-binary @- <<'JSON'
{"provider":"email","identifier":"developer@example.com","code":"<SIX_DIGIT_CODE>","password":"<YOUR_12_TO_128_CHARACTER_PASSWORD>","name":"Developer"}
JSON
)
ACCESS_TOKEN=$(printf '%s' "$LOGIN" | jq -er 'select(.code == 0) | .data.accessToken')
unset LOGIN
auth_api '/auth/me'
```

手机号注册使用同样两步：把两次请求的 `provider` 都改为 `phone`，`identifier` 都改为本人大陆手机号（11 位或 `+86` 前缀）。例如请求体形状：

```json
{"purpose":"register","provider":"phone","identifier":"<YOUR_MAINLAND_PHONE>"}
```

```json
{"provider":"phone","identifier":"<YOUR_MAINLAND_PHONE>","code":"<SIX_DIGIT_CODE>","password":"<YOUR_12_TO_128_CHARACTER_PASSWORD>","name":"Developer"}
```

验证码不能跨用途使用，也不能重复消费。同一目标发送间隔至少 60 秒，验证码有效期 5 分钟、最多尝试 5 次。身份已存在返回 `33001`，应登录或找回密码。

## 3. 密码登录、验证码登录和刷新

密码登录（手机号同样改 provider/identifier）：

```bash
LOGIN=$(api '/auth/login' --data-binary @- <<'JSON'
{"provider":"email","identifier":"developer@example.com","password":"<YOUR_12_TO_128_CHARACTER_PASSWORD>"}
JSON
)
ACCESS_TOKEN=$(printf '%s' "$LOGIN" | jq -er 'select(.code == 0) | .data.accessToken')
unset LOGIN
```

验证码登录先调用 `/auth/codes`，purpose 改为 `login`，再发送以下 JSON 到 `/auth/login`；用上面相同的方式读取 access token。password 和 code 只能提交一项：

```json
{"provider":"email","identifier":"developer@example.com","code":"<SIX_DIGIT_CODE>"}
```

登录响应业务字段示意（省略模型时间字段）：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "accessToken": "<ACCESS_TOKEN>",
    "tokenType": "Bearer",
    "expiresIn": 900,
    "user": {
      "id": "<USER_ID>", "name": "Developer",
      "systemAdmin": false, "disabled": false, "mustChangePassword": false
    }
  }
}
```

刷新只使用 Cookie，不带旧的 Bearer；必须串行执行。成功后旧 access/refresh 都失效，立刻替换内存中的 access token：

```bash
LOGIN=$(api '/auth/refresh' --request POST)
ACCESS_TOKEN=$(printf '%s' "$LOGIN" | jq -er 'select(.code == 0) | .data.accessToken')
unset LOGIN
```

刷新不会延长 30 天会话绝对期限，也不会重置“最近 5 分钟验证”时间。若失败，清空内存 token 并重新登录。初始管理员登录后若 `mustChangePassword=true`，先执行第 5 节改密，再重新登录。

## 4. GitHub / Google 浏览器授权

OAuth 必须在实际前端浏览器中发起：start 写入的 HttpOnly Cookie 要由同一浏览器带到 callback。不要在 curl 中 start 后把授权 URL 交给另一个浏览器完成。部署配置的 redirectURI 指向前端回调页，不是后端 POST 接口。

下面是前端 JavaScript，两个提供方共用流程。请求函数检查 HTTP 状态与业务 code；登录 token 只写入内存。浏览器自动发送 Origin，无需手动设置。

```js
const API = 'https://api.example.com/api/v1';
let accessToken;

async function accountRequest(path, body, authenticated = false) {
  const headers = {'Content-Type': 'application/json'};
  if (authenticated) headers.Authorization = `Bearer ${accessToken}`;
  const response = await fetch(API + path, {
    method: 'POST', credentials: 'include', headers,
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const result = await response.json();
  if (!response.ok || result.code !== 0) {
    throw new Error(`Account API failed: ${response.status}/${result.code}`);
  }
  return result.data;
}

async function startOAuth(provider, link = false) {
  // provider 只能来自页面预置的 github/google 按钮。
  if (!['github', 'google'].includes(provider)) throw new Error('Invalid provider');
  const data = await accountRequest(`/auth/oauth2/${provider}/start`, {link}, link);
  // 只保存非敏感的流程标记；不存 access token、state 或 code。
  sessionStorage.setItem(`eruun.oauth.${provider}.link`, String(link));
  location.assign(data.authorizationURL);
}

// 登录页按钮分别调用 startOAuth('github') 或 startOAuth('google')。
// 绑定按钮需先近期登录，再调用 startOAuth('github', true)。
```

在对应提供方的前端回调页执行一次以下函数；provider 由前端路由固定。不要由页面框架重复触发 callback：state 是一次性的，失败应重新 start。

```js
async function completeOAuth(provider) {
  if (!['github', 'google'].includes(provider)) throw new Error('Invalid provider');
  const params = new URLSearchParams(location.search);
  const state = params.get('state');
  const error = params.get('error');
  const code = params.get('code');
  history.replaceState(null, '', location.pathname);
  const key = `eruun.oauth.${provider}.link`;
  const link = sessionStorage.getItem(key) === 'true';
  sessionStorage.removeItem(key);
  if (!state || (!code && !error)) throw new Error('Incomplete OAuth callback');

  if (link && !error) {
    // 整页跳转会丢失内存 token；refresh 恢复同一会话，不创建新会话。
    const session = await accountRequest('/auth/refresh');
    accessToken = session.accessToken;
  }
  const result = await accountRequest(`/auth/oauth2/${provider}/callback`,
    error ? {error, state} : {code, state}, link && !error);
  if (!link) accessToken = result.accessToken;
  // link=true 成功时 result 为 null；此时刷新身份列表，不读取 result.accessToken。
}
```

授权取消也提交 `{error,state}` 以消费流程，预期得到认证失败，由 UI 显示“授权已取消”。遇到 `33002`，先登录已有账号，再重新以 `link=true` 发起授权；不能重复提交旧回调。绑定 start 与 callback 必须是同一个本地会话，且仍在最近 5 分钟验证窗口内。若超时或期间重新登录，应从 start 重来。

## 5. 改密、找回密码和身份管理

改密要求近期登录；没有 oldPassword 字段。成功会撤销全部会话，之后重新执行密码登录：

```bash
auth_api '/auth/password' --request PUT --data-binary @- <<'JSON'
{"password":"<NEW_12_TO_128_CHARACTER_PASSWORD>"}
JSON
unset ACCESS_TOKEN
```

忘记密码时无需先登录，发送 reset 验证码，再提交新密码；成功也需要重新登录：

```bash
api '/auth/codes' --data-binary @- <<'JSON'
{"purpose":"reset","provider":"email","identifier":"developer@example.com"}
JSON
```

```bash
api '/auth/password/reset' --data-binary @- <<'JSON'
{"provider":"email","identifier":"developer@example.com","code":"<SIX_DIGIT_CODE>","password":"<NEW_12_TO_128_CHARACTER_PASSWORD>"}
JSON
```

在近期登录会话中绑定邮箱（例如手机号注册后需要接受团队邮件邀请）：

```bash
auth_api '/auth/codes' --data-binary @- <<'JSON'
{"purpose":"bind","provider":"email","identifier":"developer@example.com"}
JSON
```

```bash
auth_api '/auth/identities' --data-binary @- <<'JSON'
{"provider":"email","identifier":"developer@example.com","code":"<SIX_DIGIT_CODE>"}
JSON
auth_api '/auth/identities'
```

绑定手机号同样使用 provider=phone。每个用户每个 provider 只能有一个身份；已绑定的邮箱/手机号不能直接覆盖。解绑使用身份列表的 `id`，不是 subject 或用户 ID，且不能移除最后一种可用登录方式：

```bash
IDENTITY_ID='<IDENTITY_ID_FROM_LIST>'
auth_api "/auth/identities/$IDENTITY_ID" --request DELETE
```

## 6. 创建团队、切换空间和访问资源

先列出个人及团队空间，再创建团队。注意列表项包在 `workspace` 中，创建响应则直接返回 Workspace：

```bash
auth_api '/workspaces'
TEAM=$(auth_api '/workspaces' --data-binary @- <<'JSON'
{"name":"Demo team"}
JSON
)
WORKSPACE_ID=$(printf '%s' "$TEAM" | jq -er 'select(.code == 0) | .data.id')
WORKSPACE_NAMESPACE=$(printf '%s' "$TEAM" | jq -er 'select(.code == 0) | .data.namespace')
unset TEAM
auth_api "/workspaces/$WORKSPACE_ID"
auth_api '/applications?page=1&pageSize=20' \
  --header "X-Eruun-Workspace-ID: $WORKSPACE_ID"
```

业务请求始终发送当前选中的空间 ID；省略时使用个人空间。空间管理路径里的 ID 已确定目标，无需再发送空间选择头。创建团队不会创建 Kubernetes namespace。

创建/保存应用沿用 [应用请求文档](../../docs/create-and-exec-application-api.md)，带上相同空间请求头；请求中的 namespace 若填写，须使用服务端返回的 `WORKSPACE_NAMESPACE`。不要使用旧例子中的 default namespace。首次实际部署才初始化空间安全基线，单纯保存应用不会创建 Kubernetes 资源。

## 7. 邀请成员、接受邀请和角色管理

由 owner/admin 发出邀请；目标邮箱可以尚未注册。owner 可邀请 admin/member/viewer，admin 只能邀请 member/viewer：

```bash
auth_api "/workspaces/$WORKSPACE_ID/invitations" --data-binary @- <<'JSON'
{"email":"teammate@example.com","role":"member"}
JSON
```

成功 `data.id` 是邀请 ID，用于撤销；响应没有邀请 token。token 只在目标邮箱收到的链接 `#invitation=...` 中，有效期 7 天。同一邮箱重新邀请会使旧链接失效。

接受方使用独立浏览器/会话，注册或登录拥有已验证 `teammate@example.com` 邮箱身份的账号；手机号账号先按第 5 节绑定该邮箱。清除页面 fragment 后再提交其中的 token，不要把它当成 URL query 发给 API：

```bash
# 此处 ACCESS_TOKEN 必须属于接受邀请的人。
auth_api '/workspace-invitations/accept' --data-binary @- <<'JSON'
{"token":"<TOKEN_FROM_INVITATION_FRAGMENT>"}
JSON
```

成功返回 Workspace；同一接受者仍在团队中时重复接受保持幂等。邮箱不匹配会被拒绝，不会自动切换账号或绑定邮箱。

回到 owner/admin 会话后管理成员：

```bash
auth_api "/workspaces/$WORKSPACE_ID/members"
USER_ID='<USER_ID_FROM_MEMBER_LIST>'
auth_api "/workspaces/$WORKSPACE_ID/members/$USER_ID" --request PATCH --data-binary @- <<'JSON'
{"role":"viewer"}
JSON
```

`USER_ID` 取成员列表的 `userId`。降为 viewer 后，后续请求只能读取应用元信息和状态，配置、日志、Shell、文件、模板等均被拒绝。

以下为独立管理操作，按需选择，不要连续全部执行：

```bash
# 移除成员；用自己的 userId 表示退出，但 owner 必须先转移所有权。
auth_api "/workspaces/$WORKSPACE_ID/members/$USER_ID" --request DELETE

# owner 近期登录后转移给仍在团队内且未停用的成员；原 owner 变为 admin。
auth_api "/workspaces/$WORKSPACE_ID/transfer" --data-binary @- <<'JSON'
{"userId":"<EXISTING_MEMBER_USER_ID>"}
JSON

# owner/admin 修改团队名称。
auth_api "/workspaces/$WORKSPACE_ID" --request PATCH --data-binary @- <<'JSON'
{"name":"Renamed demo team"}
JSON

INVITATION_ID='<INVITATION_ID>'
auth_api "/workspaces/$WORKSPACE_ID/invitations/$INVITATION_ID" --request DELETE

# 当前 owner 近期登录，且团队无应用、运行任务和 namespace 业务资源时才可删除。
auth_api "/workspaces/$WORKSPACE_ID" --request DELETE
```

个人空间不能邀请成员、转移所有权或删除。

## 8. 管理员、登出和常见错误

完成首次改密的系统管理员可以分页查询或停用普通用户。团队 owner 不因此获得系统管理员权限：

```bash
auth_api '/admin/users?page=1&pageSize=20'
USER_ID='<NON_ADMIN_USER_ID>'
auth_api "/admin/users/$USER_ID" --request PATCH --data-binary @- <<'JSON'
{"disabled":true}
JSON
```

停用会撤销该账号所有会话；启用使用 `{"disabled":false}`，原会话不会恢复。登出当前会话：

```bash
auth_api '/auth/logout' --request POST
unset ACCESS_TOKEN
```

| HTTP / code | 接入处理 |
| --- | --- |
| 401 | Bearer、会话、OAuth 凭据无效；可尝试一次串行 refresh，失败后重新登录；取消的 OAuth 流程重新 start |
| 403 | 检查 Origin、空间成员资格及操作角色，不要靠切换客户端 role 绕过 |
| 400 / 33000 | 检查字段名、验证码/手机号格式和密码长度；接口拒绝未知 JSON 字段 |
| 409 / 33001 | 身份/空间冲突，不要重复注册或覆盖现有绑定 |
| 409 / 33002 | 登录已有账号后重新发起 OAuth 绑定 |
| 401 / 33003 | 验证码无效、过期或耗尽；按发送间隔重新获取 |
| 429 / 33004 | 停止自动重试，等待相应目标/IP 限流窗口 |
| 403 / 33005 | 重新验证身份；refresh 不能满足近期登录要求 |
| 403 / 33006 | 初始管理员必须改密，随后重新登录 |
| 502 / 33007 | 邮件/短信发送失败，不能展示已发送成功 |
| 409 / 33008 | 团队非空，先通过正常生命周期清理资源和任务 |

上面示例不包含真实凭据；OAuth、邮件、短信投递与集群效果需要在部署方提供的环境联调。完整错误封装见 [API 错误响应契约](../../docs/api-error-response-contract.md)。
