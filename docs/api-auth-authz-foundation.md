# API 鉴权/授权一期

> 状态：Implemented Reference。JWT/RBAC 中间件已接入全局路由，但默认 `apiAuth.enabled=false`；OAuth 登录路由与 `/api/v1/authz/*` 管理路由当前未注册。

## 目标
- 为 APIServer 增加可复用的 JWT 鉴权与路由级 RBAC 授权能力。
- 配置来源为 `system setting` 的 `apiAuth` 类型。
- 当前 `master` 已在 `server.RegisterAPIRoute` 中注册 `middleware.Auth(...)`；默认系统设置会创建 `apiAuth.enabled=false`，因此默认不拦截业务请求。
- 当前 `master` 还临时注释了相关路由注册：`/api/v1/authz/*` 与 `/api/v1/auth/oauth2/google/*` 未对外暴露（访问返回 `404`）。

## 交付内容
- 新增 `apiAuth` 设置类型与校验规则：
  - `enabled=false` 时允许仅存储开关。
  - `enabled=true` 时校验 JWT 与授权规则完整性。
  - 支持 JWT 算法：`HS256`、`RS256`。
- 新增 `pkg/apiserver/interfaces/api/auth`：
  - `SystemSettingPolicyProvider`：从 `system setting` 加载策略；全局鉴权路径禁用本地缓存，确保策略变更立即生效。
  - `JWTAuthenticator`：校验 JWT 并抽取 `Principal`。
  - `RouteAuthorizer`：按 `method + path` 做角色鉴权。
- 新增 `pkg/apiserver/interfaces/api/middleware/auth.go`：
  - 提供可挂载的 gin middleware。
  - 默认放行 `/api/v1/health|healthz|ready|readyz`。
  - 返回策略：认证失败 `401`，授权失败 `403`。

## 关键策略
- 角色 claim：优先读取 `roles[]`，回退 `role`。
- 路由未命中授权策略时：`defaultEffect=deny`（未配置时也按 deny）。
- JWT 无 token 或非法 token：统一 `401`，不暴露细节。
- `apiAuth` 成功写入后，所有副本的后续非跳过路径请求都会读取并执行新策略；已在途且已加载旧策略的请求不回溯重判。
- 跳过鉴权的默认路径：`/api/v1/health`、`/api/v1/healthz`、`/api/v1/ready`、`/api/v1/readyz`，以及当前未注册的 Google OAuth2 登录/回调路径。

## 启用方式
1. 通过 `/api/v1/settings` 创建 `type=apiAuth` 的配置。
2. 设置 `value.enabled=true`，并提供完整 JWT 校验配置与授权规则。
3. 逐步按路由补全授权策略后再生产启用。

> 注意：`/api/v1/authz/*` 便捷管理路由当前未注册。授权策略的维护主路径是更新 `apiAuth` 系统设置。

## 测试
- `go test ./pkg/apiserver/domain/service -run TestSystemSettingService_Validation`
- `go test ./pkg/apiserver/interfaces/api/auth ./pkg/apiserver/interfaces/api/middleware`
