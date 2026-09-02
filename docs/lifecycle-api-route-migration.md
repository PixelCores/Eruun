# PR #209 生命周期 API 路由迁移说明

> 状态：Current。本文面向前端客户端改造，记录 PR #209 后应用生命周期相关 API 的旧接口、新接口、请求参数变化和响应示例。

## 概述

PR #209 收敛了两个不够清晰的生命周期路由：

- 删除应用从 action-style `POST /delete` 改为标准 `DELETE /applications/:appID`。
- 取消应用下全部 workflow tasks 从 `cancelall` 改到 app-scoped workflow tasks 路径下。

本次变更不新增兼容别名。旧 `/delete` 和 `/cancelall` 路由不再注册，前端客户端需要直接切换到新路由。

## 迁移总表

| 场景 | 旧接口 | 新接口 | 请求参数变化 | 响应参数变化 |
| --- | --- | --- | --- | --- |
| 删除应用 | `POST /api/v1/applications/:appID/delete` | `DELETE /api/v1/applications/:appID` | HTTP method 和 path 变化；path 参数 `appID` 不变；JSON body `waitSeconds` 不变且可选 | 无变化 |
| 取消应用下所有 workflow tasks | `POST /api/v1/applications/:appID/cancelall` | `POST /api/v1/applications/:appID/workflow/tasks/cancel-all` | path 变化；path 参数 `appID` 不变；无请求体 | 无变化 |
| 启动应用 | `POST /api/v1/applications/:appID/start` | `POST /api/v1/applications/:appID/start` | 路由不变；请求体可为空，也可传本次 task 级 `callback` | 无变化 |
| 停止应用 | `POST /api/v1/applications/:appID/stop` | `POST /api/v1/applications/:appID/stop` | 路由不变；请求体可为空，也可传本次 task 级 `callback` | 无变化 |
| 重启应用 | `POST /api/v1/applications/:appID/restart` | `POST /api/v1/applications/:appID/restart` | 路由不变；请求体可为空，也可传本次 task 级 `callback` | 无变化 |

所有响应仍使用统一 envelope：

```json
{
  "code": 0,
  "message": "",
  "data": {}
}
```

## 删除应用

### 旧接口

```http
POST /api/v1/applications/:appID/delete
```

### 新接口

```http
DELETE /api/v1/applications/:appID
```

### 请求参数

| 位置 | 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| path | `appID` | string | 是 | 应用 ID |
| body | `waitSeconds` | int64 | 否 | 删除前等待活跃任务取消完成的秒数；不传时使用服务端默认值；不能小于 `0` |

`DELETE` 可以不传 body。如果前端不需要覆盖等待时间，建议直接不发送请求体。

部分 HTTP 客户端默认不会在 `DELETE` 请求上发送 body。需要传 `waitSeconds` 时，请确认客户端封装支持 `DELETE` body。

### 请求示例

不传 body：

```bash
curl -X DELETE "$ERUUN_API_URL/api/v1/applications/app-123"
```

指定等待时间：

```bash
curl -X DELETE "$ERUUN_API_URL/api/v1/applications/app-123" \
  -H "Content-Type: application/json" \
  --data '{"waitSeconds":30}'
```

### 成功响应示例

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "cancelledTaskIds": ["task-1"],
    "activeTaskIds": ["task-2"],
    "deletedResources": [
      "Deployment:default/web",
      "Service:default/web"
    ],
    "failedResources": [
      "StatefulSet:default/mysql (delete timeout)"
    ],
    "warnings": [
      "timeout waiting task-2"
    ],
    "deletedCounts": {
      "schedules": 1,
      "workflows": 1,
      "components": 2,
      "tasks": 3,
      "jobs": 4,
      "apps": 1
    }
  }
}
```

字段说明：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `appId` | string | 被删除的应用 ID |
| `cancelledTaskIds` | string[] | 删除过程中已取消的任务 ID；为空时可能省略 |
| `activeTaskIds` | string[] | 等待后仍未结束的活跃任务 ID；为空时可能省略 |
| `deletedResources` | string[] | 已删除的 Kubernetes 资源；为空时可能省略 |
| `failedResources` | string[] | 删除失败的资源明细；为空时可能省略 |
| `warnings` | string[] | 非完全成功的提示；为空时可能省略 |
| `deletedCounts` | object | 删除前统计的记录数量 |

删除接口在存在 `warnings`、`activeTaskIds` 或 `failedResources` 时仍可能返回 HTTP 200，前端应展示这些明细，不要只按 HTTP 状态判断是否完全成功。

`observe` 和 `adopted` 应用的默认删除都只解除 Eruun 元数据关系，响应包含 `resourcesRetained=true`，不会进入 native 资源清理。adopted 的显式资源删除必须先调用 `POST /api/v1/applications/:appID/resources/cleanup-plan` 获取 HMAC 签名计划，再向 `DELETE /api/v1/applications/:appID/resources` 提交匹配的 `planFingerprint`；无指纹 DELETE 只保留 native 兼容行为，不能绕过 adopted 的签名校验。

## 取消应用下所有 workflow tasks

### 旧接口

```http
POST /api/v1/applications/:appID/cancelall
```

### 新接口

```http
POST /api/v1/applications/:appID/workflow/tasks/cancel-all
```

### 请求参数

| 位置 | 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| path | `appID` | string | 是 | 应用 ID |

该接口无请求体。取消人使用服务端默认值。

### 请求示例

```bash
curl -X POST "$ERUUN_API_URL/api/v1/applications/app-123/workflow/tasks/cancel-all"
```

### 成功响应示例

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "cancelledTaskIds": ["task-1", "task-2"]
  }
}
```

没有可取消任务时也返回成功：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "cancelledTaskIds": []
  }
}
```

## 启动、停止、重启应用

`start`、`stop`、`restart` 的路由在 PR #209 后不变；响应字段不变。请求体继续兼容空 body，也可以提供本次操作 task 级 `callback`。

### 当前接口

```http
POST /api/v1/applications/:appID/start
POST /api/v1/applications/:appID/stop
POST /api/v1/applications/:appID/restart
```

这三个接口都使用 path 参数 `appID`。请求体可为空；如需只对本次生命周期操作触发终态回调，可传入可选 `callback`：

```json
{
  "callback": {
    "success": "https://example.com/lifecycle/success",
    "failure": "https://example.com/lifecycle/failure",
    "methods": {
      "success": "POST",
      "failure": "POST"
    },
    "headers": {
      "X-Source": "eruun"
    },
    "timeoutSeconds": 30
  }
}
```

`callback` 复用 App / Workflow Callback 字段，支持 `success`、`failure`、`timeout`、`reject`、`cancelled`、`methods`、`headers`、`timeoutSeconds`。它只挂到本次 start/stop/restart operation task，不写入 App，不覆盖 Workflow callback；未传 `callback` 时不会触发生命周期操作回调，也不会回退 App callback。callback payload 中 `workflowType` 分别为 `start`、`stop`、`restart`，`workflowId` 为空。

### start 响应示例

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "taskId": "task-start-001",
    "startedAt": "2026-06-24T08:00:00Z",
    "startedResources": ["Deployment:default/web"],
    "skippedResources": [],
    "failedResources": []
  }
}
```

### stop 响应示例

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "taskId": "task-stop-001",
    "stoppedAt": "2026-06-24T08:05:00Z",
    "stoppedResources": ["Deployment:default/web"],
    "skippedResources": [],
    "failedResources": []
  }
}
```

### restart 响应示例

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "taskId": "task-restart-001",
    "restartedAt": "2026-06-24T08:10:00Z",
    "restartedResources": ["Deployment:default/web"],
    "skippedResources": [],
    "failedResources": []
  }
}
```

`taskId`、`skippedResources`、`failedResources` 为空时可能省略。前端需要兼容字段缺失和空数组两种形态。

## Adopted 生命周期契约

显式接管由 namespace adopted dry-run/apply 创建；现有 `native` 路由和响应保持兼容。

- stop、start、restart 与 workflow 调度共用应用级分布式锁。锁内会重新读取应用和组件并确认没有活动任务，避免预检与 Kubernetes 写入之间发生并发漂移。
- 每个目标必须按持久化的 source `apiVersion/kind/name/UID` 命中原 Deployment 或 StatefulSet；同名异 UID 会在写入前失败。
- 任何目标存在 HPA 时，本次生命周期请求整体拒绝。StatefulSet 还会预检 PVC：被引用的 standalone/VCT PVC 必须存在、未删除且处于 `Bound`。
- stop 保存每个 source workload 的 live replicas 后缩容；start 只恢复该快照中的正副本数，不采用可能漂移的导入配置。
- restart 使用纳秒精度的 `kubectl.kubernetes.io/restartedAt`。暂停的 Deployment、`OnDelete` StatefulSet 和非零 `rollingUpdate.partition` 会在写入前拒绝。
- 共享或受保护目标只会出现在 `skippedResources`，不会被生命周期操作改写。

adopted cleanup apply 会先 quiesce root Deployment/StatefulSet，再重扫并删除签名覆盖的 runtime child，最后删除 root 和仍为 exclusive 的非数据依赖。调用方必须先获取 cleanup plan 并提交匹配指纹。PVC/PV、shared/external 依赖始终保留；UID/resourceVersion 漂移、晚到 child、finalizer 或部分失败都会停止后续依赖删除并允许使用新计划安全重试。

## 不再支持的路径

以下路径不是当前可用接口，前端不要继续调用：

```http
POST /api/v1/applications/:appID/delete
POST /api/v1/applications/:appID/cancelall
POST /api/v1/applications/:appID/actions/start
POST /api/v1/applications/:appID/actions/stop
POST /api/v1/applications/:appID/actions/restart
```

## 前端改造清单

- 将删除应用请求从 `POST /applications/:appID/delete` 改为 `DELETE /applications/:appID`。
- 如果删除应用需要传 `waitSeconds`，确认 HTTP client 支持 `DELETE` body；否则不传 body，使用服务端默认等待时间。
- 将取消全部 workflow tasks 请求从 `POST /applications/:appID/cancelall` 改为 `POST /applications/:appID/workflow/tasks/cancel-all`。
- 保持 `start`、`stop`、`restart` 调用不变，不要迁移到 `/actions/*`。
- 删除接口返回 HTTP 200 时仍检查 `warnings`、`activeTaskIds`、`failedResources`，用于展示部分失败或未完成明细。
- 统一按 `code/message/data` envelope 解析响应。
