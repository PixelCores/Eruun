# 设置页接口接入方案（现有接口驱动）

> 状态：Current。本文面向前端设置页接入，只描述当前后端已经实现的 `/api/v1/settings` 能力，不从设计稿或本地偏好反推新接口。

登录和空间管理使用 [账号与空间接入文档](account-auth-workspaces.md)。本页接口仅系统管理员可访问；认证配置从部署 Secret 加载。

## Summary

- 设置页 v1 只管理 `eruun_system_setting` 当前支持的两种 `type`：`urlSecurityPolicy`、`podRestartMonitor`。
- 页面初始化使用 `GET /api/v1/settings`，按 `settings[].type` 分发到对应 UI 模块。
- 单模块刷新使用 `GET /api/v1/settings/{type}`；已有记录保存使用 `PUT /api/v1/settings/{type}`；缺失记录只在确认 404 后使用 `POST /api/v1/settings` 创建。
- `projectName`、`defaultNamespace`、默认资源限制不是当前 system setting 类型；如果前端需要这些值，只能作为前端本地偏好；namespace 由选中的空间确定，不能提交到 `/api/v1/settings`。

## Page Structure

| 页面模块 | setting type | 主要表单字段 | 建议交互 |
| --- | --- | --- | --- |
| Security Policy | `urlSecurityPolicy` | `allowPrivateByDefault`、`allowedHostPatterns`、`allowedCIDRs` | 可编辑；用于出站 URL 私网访问策略 |
| Runtime Monitoring | `podRestartMonitor` | `enabled`、`windowSeconds`、`threshold` | 可编辑；用于 Pod 重启阈值监控 |

## Common API Flow

加载整个设置页：

```http
GET /api/v1/settings
```

返回体中的 `data.settings` 按 `type` 分发，例如：

```json
{
  "settings": [
    {
      "type": "urlSecurityPolicy",
      "value": {
        "allowPrivateByDefault": false,
        "allowedHostPatterns": ["*.svc.cluster.local", "*.paas.example.com"],
        "allowedCIDRs": []
      },
      "createTime": "2026-06-17T00:00:00Z",
      "updateTime": "2026-06-17T00:00:00Z"
    }
  ]
}
```

刷新单个模块：

```http
GET /api/v1/settings/{type}
```

更新已存在配置：

```http
PUT /api/v1/settings/{type}
Content-Type: application/json

{"value": {...}}
```

创建缺失配置：

```http
POST /api/v1/settings
Content-Type: application/json

{"type":"{type}","value": {...}}
```

删除配置：

```http
DELETE /api/v1/settings/{type}
```

保存策略：

- `urlSecurityPolicy`、`podRestartMonitor` 启动时会自动补齐默认记录，通常直接 `PUT`。
- `POST` 请求体必须是 `{"type":"...","value":...}`；`PUT` 请求体必须是 `{"value":...}`，不要在 `PUT` 里重复提交 `type`。
- 保存失败时展示后端错误，不要在前端伪造成功状态。

## Module Contracts

### Security Policy: `urlSecurityPolicy`

表单建议：

- `allowPrivateByDefault`: boolean
- `allowedHostPatterns`: string list，支持精确主机与 `*.` 通配后缀
- `allowedCIDRs`: CIDR string list

保存示例：

```http
PUT /api/v1/settings/urlSecurityPolicy
Content-Type: application/json

{
  "value": {
    "allowPrivateByDefault": false,
    "allowedHostPatterns": [
      "*.svc.cluster.local",
      "*.paas.example.com"
    ],
    "allowedCIDRs": []
  }
}
```

### Runtime Monitoring: `podRestartMonitor`

表单建议：

- `enabled`: boolean
- `windowSeconds`: number，必须大于 0
- `threshold`: number，必须大于 0

保存示例：

```http
PUT /api/v1/settings/podRestartMonitor
Content-Type: application/json

{
  "value": {
    "enabled": true,
    "windowSeconds": 1800,
    "threshold": 3
  }
}
```

## Not Current System Settings

以下字段可以存在于前端界面或创建应用表单中，但不是当前 `/api/v1/settings` 后端配置：

| 字段 | 当前处理方式 |
| --- | --- |
| `projectName` | 作为前端本地偏好；创建应用时写入 `POST /api/v1/applications` 的 `project` |
| `defaultNamespace` | 作为前端本地偏好；创建应用时写入应用和组件的 `namespace` |
| 默认 `replicas` / CPU / Memory / GPU | 作为新建组件表单默认值；创建应用时写入组件的 `replicas` 与 `traits.resources` |

不要把这些字段提交为任何未实现的 setting type。当前后端会拒绝未知 setting type，并返回 `system setting type is invalid`。

## Test Plan

- 页面加载时 `GET /api/v1/settings` 能按两种已实现 `type` 填充对应模块。
- 每个模块只保存自己的 `type`，不覆盖其他 setting。
- 所有保存请求都匹配 DTO：create 使用 `type + value`，update 只使用 `value`。
- 文档和前端配置中不把任何未实现 setting type 描述为可用配置。

## Assumptions

- v1 不新增后端接口，不新增 setting type。
- 设置页目标是管理当前已经支持的系统设置，不承担 workspace preference 持久化。
