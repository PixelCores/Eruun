# 设置页接口接入方案（现有接口驱动）

> 状态：Current。本文面向前端设置页接入，只描述当前后端已经实现的 `/api/v1/settings` 能力，不从设计稿或本地偏好反推新接口。

登录和空间管理使用 [账号与空间接入文档](account-auth-workspaces.md)。本页接口仅系统管理员可访问；认证配置从部署 Secret 加载。

## Summary

- 设置页 v1 只管理 `eruun_system_setting` 当前支持的五种 `type`：`nodeSelector`、`rbacPolicies`、`aliyunCloud`、`urlSecurityPolicy`、`podRestartMonitor`。
- 页面初始化使用 `GET /api/v1/settings`，按 `settings[].type` 分发到对应 UI 模块。
- 单模块刷新使用 `GET /api/v1/settings/{type}`；已有记录保存使用 `PUT /api/v1/settings/{type}`；缺失记录只在确认 404 后使用 `POST /api/v1/settings` 创建。
- `projectName`、`defaultNamespace`、默认资源限制不是当前 system setting 类型；如果前端需要这些值，只能作为前端本地偏好；namespace 由选中的空间确定，不能提交到 `/api/v1/settings`。

## Page Structure

| 页面模块 | setting type | 主要表单字段 | 建议交互 |
| --- | --- | --- | --- |
| Scheduling | `nodeSelector` | `nodeSelector`、`affinity`、`tolerations` | Key/value + JSON 编辑；用于维护集群调度选择配置 |
| Access Control | `rbacPolicies` | policy 数组、`serviceAccount`、RBAC rules | 数组编辑；适合表格 + JSON 兜底编辑 |
| Security Policy | `urlSecurityPolicy` | `allowPrivateByDefault`、`allowedHostPatterns`、`allowedCIDRs` | 可编辑；用于出站 URL 私网访问策略 |
| Runtime Monitoring | `podRestartMonitor` | `enabled`、`windowSeconds`、`threshold` | 可编辑；用于 Pod 重启阈值监控 |
| Cloud Provider | `aliyunCloud` | AK/SK、`endpoint`、`regionId`、`zoneId`、`vpcId`、`vswId` | 可编辑；保存前后端会做阿里云 NAS 连通性校验 |

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
- `nodeSelector`、`rbacPolicies` 可由初始化 SQL 补齐；前端遇到 404 时再 `POST` 创建。
- `aliyunCloud` 不会自动初始化；前端遇到 404 时再 `POST` 创建，存在时使用 `PUT`。
- `POST` 请求体必须是 `{"type":"...","value":...}`；`PUT` 请求体必须是 `{"value":...}`，不要在 `PUT` 里重复提交 `type`。
- 保存失败时展示后端错误，不要在前端伪造成功状态；尤其是 `aliyunCloud`，配置错误或 NAS 只读权限不足会导致后端拒绝落库。

## Module Contracts

### Scheduling: `nodeSelector`

表单建议：

- `nodeSelector`: key/value map，例如 `node.kubernetes.io/workload=gpu`
- `affinity`: JSON object
- `tolerations`: JSON array

保存示例：

```http
PUT /api/v1/settings/nodeSelector
Content-Type: application/json

{
  "value": {
    "nodeSelector": {
      "node.kubernetes.io/workload": "gpu"
    },
    "affinity": {},
    "tolerations": []
  }
}
```

### Access Control: `rbacPolicies`

表单建议：

- policy list
- `serviceAccount`
- `rules[].apiGroups`
- `rules[].resources`
- `rules[].verbs`

`value` 必须是 JSON array。

保存示例：

```http
PUT /api/v1/settings/rbacPolicies
Content-Type: application/json

{
  "value": [
    {
      "serviceAccount": "default",
      "rules": [
        {
          "apiGroups": [""],
          "resources": ["pods"],
          "verbs": ["get", "list"]
        }
      ]
    }
  ]
}
```

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

### Cloud Provider: `aliyunCloud`

表单建议：

- `accessKeyId`
- `accessKeySecret`
- `endpoint`
- `regionId`
- `zoneId`
- `vpcId`
- `vswId`

读取响应会对 `accessKeySecret` 脱敏为 `******`。保存时必须提交真实 `accessKeySecret`，不能提交脱敏占位值。后端保存前会调用阿里云 NAS `DescribeFileSystems` 做只读连通性校验。

创建示例：

```http
POST /api/v1/settings
Content-Type: application/json

{
  "type": "aliyunCloud",
  "value": {
    "accessKeyId": "replace-with-access-key-id",
    "accessKeySecret": "replace-with-access-key-secret",
    "endpoint": "nas.cn-hangzhou.aliyuncs.com",
    "regionId": "cn-hangzhou",
    "zoneId": "cn-hangzhou-i",
    "vpcId": "vpc-xxxx",
    "vswId": "vsw-xxxx"
  }
}
```

更新示例：

```http
PUT /api/v1/settings/aliyunCloud
Content-Type: application/json

{
  "value": {
    "accessKeyId": "replace-with-access-key-id",
    "accessKeySecret": "replace-with-access-key-secret",
    "endpoint": "nas.cn-hangzhou.aliyuncs.com",
    "regionId": "cn-hangzhou",
    "zoneId": "cn-hangzhou-i",
    "vpcId": "vpc-xxxx",
    "vswId": "vsw-xxxx"
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

- 页面加载时 `GET /api/v1/settings` 能按五种已实现 `type` 填充对应模块。
- 每个模块只保存自己的 `type`，不覆盖其他 setting。
- 所有保存请求都匹配 DTO：create 使用 `type + value`，update 只使用 `value`。
- 文档和前端配置中不把任何未实现 setting type 描述为可用配置。
- 密钥字段读取后显示脱敏值，但未重新输入真实密钥时不允许提交保存。
- `aliyunCloud` 保存失败时展示后端连通性错误，不在前端伪造成功状态。

## Assumptions

- v1 不新增后端接口，不新增 setting type。
- 设置页目标是管理当前已经支持的系统设置，不承担 workspace preference 持久化。
