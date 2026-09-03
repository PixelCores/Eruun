# 系统设置

> 状态：Current。所有 `/api/v1/settings` 接口必须 Bearer 登录且为系统管理员。账号、OAuth、会话及空间策略由部署 Secret 配置，见 [账号与空间](account-auth-workspaces.md)。

## 数据与接口

`eruun_system_setting` 使用 `type` 唯一键和 JSON `value`。当前支持五种配置：

| type | value 契约 |
| --- | --- |
| `nodeSelector` | 调度 nodeSelector、affinity、tolerations 对象或 profile 数组 |
| `rbacPolicies` | RBAC policy 数组，面向系统管理；普通空间拒绝 RBAC trait |
| `aliyunCloud` | 云适配器配置；普通空间拒绝 CloudJob |
| `urlSecurityPolicy` | 服务端出站 URL 私网白名单，见 [URL 安全策略](url-security-policy.md) |
| `podRestartMonitor` | `{enabled,windowSeconds,threshold}`，默认 true、1800、3 |

| 方法 / 路径 | 请求 / 返回 |
| --- | --- |
| `GET /api/v1/settings` | `data.settings` 配置列表 |
| `GET /api/v1/settings/:type` | 单条配置，不存在时返回 404 |
| `POST /api/v1/settings` | `{type,value}` 创建 |
| `PUT /api/v1/settings/:type` | `{value}` 更新 |
| `DELETE /api/v1/settings/:type` | 删除指定配置 |

例如：

```sh
curl --fail "$SERVER/api/v1/settings/urlSecurityPolicy" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

`aliyunCloud` 包含 accessKeyId、accessKeySecret、endpoint、regionId、zoneId、vpcId、vswId。读取只返回掩码密钥，写入必须提供真实值，不能提交掩码。保存前调用阿里云 NAS 只读接口验证凭据和默认区域，失败不落库。该云配置和账号登录短信配置相互独立。

运行时只自动补齐 `urlSecurityPolicy` 和 `podRestartMonitor` 默认记录。`scripts/init-system-setting.sql` 可幂等补齐其余设置，保留现有值。前端字段和保存交互见 [设置页接入](settings-page-api.md)。

旧认证设置类型、JWT 角色映射及动态 authz 路由接口已移除；新接入必须使用统一账号和空间 API。
