# 设置页接口接入方案（现有接口驱动）

> 状态：Current。本文面向前端设置页接入，描述当前后端已经实现的 `/api/v1/settings`、节点池能力，不从设计稿或本地偏好反推新接口。

登录和空间管理使用 [账号与空间接入文档](account-auth-workspaces.md)。系统设置和配置管理接口仅系统管理员可访问；节点池另提供空间成员只读选项接口。认证配置从部署 Secret 加载。

## Summary

- 设置页 v1 管理 `eruun_system_setting` 的三种 `type`：`urlSecurityPolicy`、`podRestartMonitor`、`workflow_scheduler`。
- 节点池通过[节点池](#节点池)一节的 `/api/v1/admin/node-pools` 接口管理，组件通过 `traits.nodePool` 引用。
- 页面初始化使用 `GET /api/v1/settings`，按 `settings[].type` 分发到对应 UI 模块。
- 单模块刷新使用 `GET /api/v1/settings/{type}`；已有记录保存使用 `PUT /api/v1/settings/{type}`；缺失记录只在确认 404 后使用 `POST /api/v1/settings` 创建。
- `projectName`、`defaultNamespace`、默认资源限制不是当前 system setting 类型；如果前端需要这些值，只能作为前端本地偏好；namespace 由选中的空间确定，不能提交到 `/api/v1/settings`。

## Page Structure

| 页面模块 | setting type | 主要表单字段 | 建议交互 |
| --- | --- | --- | --- |
| Security Policy | `urlSecurityPolicy` | `allowPrivateByDefault`、`allowedHostPatterns`、`allowedCIDRs` | 可编辑；用于出站 URL 私网访问策略 |
| Runtime Monitoring | `podRestartMonitor` | `enabled`、`windowSeconds`、`threshold` | 可编辑；用于 Pod 重启阈值监控 |
| Job Scheduler | `workflow_scheduler` | 全局/空间并发、优先级、评测时长和资源创建预算 | 见[系统设置](system-setting.md#job-全局调度)；不可删除 |

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

- `urlSecurityPolicy`、`podRestartMonitor`、`workflow_scheduler` 启动时会自动补齐默认记录，通常直接 `PUT`；`workflow_scheduler` 不可删除。
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

- 页面加载时 `GET /api/v1/settings` 能按三种已实现 `type` 填充对应模块。
- 每个模块只保存自己的 `type`，不覆盖其他 setting。
- 所有保存请求都匹配 DTO：create 使用 `type + value`，update 只使用 `value`。
- 文档和前端配置中不把任何未实现 setting type 描述为可用配置。

## Assumptions

- v1 不新增后端接口，不新增 setting type。
- 设置页目标是管理当前已经支持的系统设置，不承担 workspace preference 持久化。


## 节点池

节点池由系统管理员统一维护，对全部空间提供名字和描述。配置存储在 MySQL 的 `eruun_node_pools` 表，以 `name` 为主键，不使用缓存。组件通过 `traits.nodePool` 引用，写入时校验存在，部署时从 MySQL 解析。调度配置通过本节接口维护，不再作为系统设置管理；组件不提供 Kubernetes RBAC 配置。

### 管理员接口

所有管理接口要求 Bearer 登录且用户为系统管理员（`minimum = "system"`），空间管理员仍无权访问。列表按名字升序返回。

| 方法 | 节点池 | 成功响应的 data |
| --- | --- | --- |
| GET | `/api/v1/admin/node-pools` | `{"nodePools": [NodePool]}` |
| POST | `/api/v1/admin/node-pools` | 201，对象本身 |
| GET | `/api/v1/admin/node-pools/:name` | 200，对象本身 |
| PUT | `/api/v1/admin/node-pools/:name` | 200，更新后的对象 |
| DELETE | `/api/v1/admin/node-pools/:name` | 204，无响应体 |

除 204 外，成功和错误响应使用统一的 `{code, message, data}` envelope。请求严格拒绝未知字段。`name` 创建后不可修改；PUT 主体不允许携带 `name`，仅以路径参数标识对象。PUT 整体替换所有可变字段，省略 `description` 或 `tolerations` 会清空它们；必需的 `nodeSelector` 不可省略。

创建节点池：

```http
POST /api/v1/admin/node-pools
Content-Type: application/json
Authorization: Bearer <access-token>

{
  "name": "gpu-a100",
  "description": "A100 推理节点",
  "nodeSelector": {"eruun.io/pool": "gpu-a100"},
  "tolerations": [{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}]
}
```

201 响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "name": "gpu-a100",
    "description": "A100 推理节点",
    "nodeSelector": {"eruun.io/pool": "gpu-a100"},
    "tolerations": [{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}],
    "createTime": "2026-10-21T00:00:00Z",
    "updateTime": "2026-10-21T00:00:00Z"
  }
}
```

管理员列表和单项查询返回完整配置。删除不检查引用，也不触发部署。

### 成员只读接口

`GET /api/v1/node-pools` 要求已登录并具有当前空间的 `viewer` 或更高角色；使用 `X-Eruun-Workspace-ID` 选择空间，缺省使用个人空间。此接口只返回名字和描述，不返回 `nodeSelector`、`tolerations` 或时间字段。

```json
{"code":0,"message":"","data":{"nodePools":[{"name":"gpu-a100","description":"A100 推理节点"}]}}
```


空列表为 `[]`；没有描述时成员选项的 `description` 为 `""`。

### 校验和错误码

- `name` 必须为 DNS-1123 label，最长 63 字符；不修剪或自动改写名字。
- `nodeSelector` 至少一项；键必须为合法的 Kubernetes 标签键，值必须为合法且非空的标签值。
- `tolerations[].operator` 必须显式为 `Equal` 或 `Exists`；`Exists` 的 `value` 必须为空；`Equal` 的 `key` 不能为空；非空 `key` 必须为合法标签键。`effect` 只能为空、`NoSchedule`、`PreferNoSchedule` 或 `NoExecute`；仅 `NoExecute` 可设置 `tolerationSeconds`。

校验失败返回 400，消息指出具体字段，例如 `tolerations[0].operator`。`34xxx` 已被 Job 错误码占用，配置使用 `35xxx`：

| 错误 | HTTP | 业务码 |
| --- | --- | --- |
| `ErrNodePoolInvalid` | 400 | 35000 |
| `ErrNodePoolExists` | 409 | 35001 |
| `ErrNodePoolNotFound` | 404 | 35002 |

重名创建返回 Exists；查询、更新或删除不存在的合法名字返回 NotFound。

启动 schema 迁移会自动创建节点池表。旧 Profile 导入及删表迁移已移除，本次不搬迁旧配置数据。
