# Google OAuth2 登录（Phase 1）

OAuth state 使用缓存后端的原子读取并删除操作消费：内存后端在同一互斥区内完成，Redis 后端使用 `GETDEL`。同一个 state 即使被并发回调也只有一个请求能取得 PKCE verifier；后端消费失败时登录流程按无效 state 关闭失败。

> 状态：Implemented Reference。当前后端流程已实现，但 OAuth 路由在 `master` 未注册。

> 注意：当前 `master` 临时未注册 `/api/v1/auth/oauth2/google/login` 与 `/api/v1/auth/oauth2/google/callback`，访问会返回 `404`。本文档记录已实现但尚未对外暴露的接入流程，供后续恢复路由时使用。

## 概述
- 实现方式：Authorization Code + PKCE（S256）
- 恢复启用登录路径：`GET /api/v1/auth/oauth2/google/login`
- 恢复启用回调路径：`GET /api/v1/auth/oauth2/google/callback`
- 回调成功后由 APIServer 签发本地 JWT，供现有 `Authorization: Bearer` 鉴权链路使用。

## 前置配置
1. 在 Google Cloud Console 创建 OAuth2 Client。
2. 将回调地址加入 Authorized redirect URIs。
3. 通过系统设置写入 `oauthAuth`：

```bash
SERVER="http://127.0.0.1:8000"

curl -sS -X POST "${SERVER}/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-oauth-auth-setting.json
```

4. 确保 `apiAuth.jwt.hs256.secret` 已配置（本地 JWT 签发复用该 secret）。

## 调用流程

> 当前 `master` 不暴露以下路由，直接访问会返回 `404`。不要根据本节为当前前端实现可用登录 UI；本节仅用于后续恢复路由注册后的接入参考。

1. 恢复路由后，浏览器访问登录路径：

```bash
curl -i "${SERVER}/api/v1/auth/oauth2/google/login"
```

服务返回 `302 Location` 到 Google 授权页面，URL 中包含 `state` 与 `code_challenge`。

2. Google 重定向到回调地址，携带 `code` 与 `state`。
3. 回调成功后返回：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "accessToken": "<redacted-access-token>.",
    "tokenType": "Bearer",
    "expiresIn": 3600,
    "subject": "google-sub",
    "email": "owner@example.com",
    "roles": ["admin"]
  }
}
```

## 错误码
- `31001`: oauth config is invalid
- `31002`: oauth state is invalid or expired
- `31003`: oauth code exchange failed
- `31004`: oauth user info fetch failed
- `31005`: oauth role mapping failed
- `31006`: oauth token issue failed

## 安全说明
- `state` 一次性消费，且有 TTL（默认 300 秒）。
- 配置读取接口会脱敏 `providers.google.clientSecret`（返回 `******`）。
- 若直接回写脱敏值 `******`，配置校验会拒绝。
