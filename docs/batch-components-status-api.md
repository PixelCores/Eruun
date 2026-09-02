# 批量应用状态汇总 API

> 状态：Current。当前路由为 `POST /api/v1/applications/components/status`。状态全集、聚合优先级和状态扭转的主契约见 `application-status-api.md`。

## 概述

该接口用于一次查询一个或多个应用的应用级聚合状态。单应用查询也可以使用同一个请求体，把目标应用 ID 放入 `appIds` 数组即可。

- 方法：`POST`
- 路径：`/api/v1/applications/components/status`

返回值是应用级聚合状态，不是组件明细。组件级明细请使用 `GET /api/v1/applications/:appID/components/status`；单 APP 聚合状态请优先使用 `GET /api/v1/applications/:appID/status`。

运行态事实源来自 `eruun_app_components.status`、`eruun_app_components.ready_replicas`、`eruun_app_components.last_abnormal` 和组件 `update_time`。本接口直接读取组件 repository 的最新持久化快照，不读取普通 Components API 的组件缓存，并沿用既有的 ready 副本纠正规则。本接口返回的 `status` 统一为小写聚合状态，如 `running`、`starting`；组件明细接口返回组件状态，如 `Running`、`Starting`。

## 请求体

```json
{
  "appIds": [
    "app-123",
    "app-456"
  ]
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `appIds` | `string[]` | 是 | 应用 ID 数组，不能为空。 |
| `appIds[]` | `string` | 是 | 单个应用 ID。空字符串不会导致整个请求失败，会在对应结果项中返回 `error`。 |

单应用查询示例：

```bash
curl -sS http://127.0.0.1:8000/api/v1/applications/components/status \
  -H 'Content-Type: application/json' \
  -d '{"appIds":["app-123"]}'
```

## 响应结构

成功请求统一返回 HTTP 200，响应 envelope 如下：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "results": [
      {
        "appId": "app-123",
        "status": "running"
      }
    ]
  }
}
```

`data.results[]` 字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `appId` | `string` | 本条结果对应的应用 ID，服务端会去掉首尾空白。 |
| `status` | `string` | 成功查询时返回的应用聚合状态。出现 `error` 时通常不返回该字段。 |
| `error` | `string` | 本条查询失败时返回的错误信息。 |

该接口采用 partial success 模式：单个 `appId` 出错不会影响其他 `appId`，整批请求仍返回 HTTP 200。

## 聚合状态摘要

完整语义、状态优先级、状态扭转链路见 `application-status-api.md`。本接口可能返回的聚合状态如下：

| 聚合状态 | 典型来源组件状态 | 摘要 |
| --- | --- | --- |
| `failed` | `Failed` | 至少一个有效组件失败。 |
| `deploying` | `Deploying` | 初次部署 workflow 已把目标组件标记为部署中。 |
| `updating` | `Updating` 或活跃 `/version` workflow task | 组件更新中，或即时 `/version` 真实 workflow task 仍活跃。 |
| `restarting` | `Restarting` | 组件重启中。 |
| `starting` | `Starting` | `/start` 从 `Stopped` 恢复后的启动中，不用于初次部署。 |
| `cleaning` | `Cleaning` | 组件资源清理中。 |
| `pending` | `Pending` | 服务组件等待调度或副本未就绪。 |
| `running` | `Running` | 服务组件运行中。 |
| `stopped` | `Stopped` | 服务组件已停止。 |
| `not_deploy` | `Not Deploy`、空组件状态、无组件 | 应用尚未部署或无可用部署状态。 |
| `unknown` | `Unknown`、无法识别的组件状态 | 状态无法判断。 |

聚合优先级为：

```text
failed > deploying > updating > restarting > starting > cleaning > pending > running > stopped > not_deploy > unknown
```

重要边界：

- 有 managed `webservice` 时，普通可用性状态以 managed `webservice` 为主；未配置 `share` 和 `share=force` 属于 managed，`share=default` / `share=ignore` 不参与普通 availability。例如 managed backend/socket=`Stopped`、共享 proxy=`Running`、store=`Running` 时返回 `stopped`；managed backend/frontend/socket=`Running`、共享 proxy=`Pending` 时返回 `running`。
- 只有共享 webservice 时回退按共享组件的真实状态聚合，不把共享资源存在直接视为 `running`。
- `store` 等非服务组件的 `failed`、`deploying`、`updating`、`restarting`、`starting`、`cleaning` 仍会作为全局高优先级状态影响聚合结果。
- Pod 型组件的新近 `Failed` 在应用聚合层有 3 分钟平滑窗口；组件明细接口仍返回原始 `Failed` 和 `lastAbnormal`。

## 所有状态响应示例

以下示例展示所有可能的聚合状态。实际请求只会返回传入 `appIds` 对应的结果。

```json
{
  "code": 0,
  "message": "",
  "data": {
    "results": [
      {
        "appId": "app-failed",
        "status": "failed"
      },
      {
        "appId": "app-deploying",
        "status": "deploying"
      },
      {
        "appId": "app-updating",
        "status": "updating"
      },
      {
        "appId": "app-restarting",
        "status": "restarting"
      },
      {
        "appId": "app-starting",
        "status": "starting"
      },
      {
        "appId": "app-cleaning",
        "status": "cleaning"
      },
      {
        "appId": "app-pending",
        "status": "pending"
      },
      {
        "appId": "app-running",
        "status": "running"
      },
      {
        "appId": "app-stopped",
        "status": "stopped"
      },
      {
        "appId": "app-not-deploy",
        "status": "not_deploy"
      },
      {
        "appId": "app-unknown",
        "status": "unknown"
      }
    ]
  }
}
```

## stop 后查询状态示例

用户先停止应用：

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/applications/app-123/stop
```

停止接口响应示例：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "taskId": "task-stop-001",
    "stoppedAt": "2026-06-24T08:05:00Z",
    "stoppedResources": [
      "Deployment:default/web"
    ]
  }
}
```

`skippedResources` 和 `failedResources` 仅在存在跳过或失败资源时返回。

停止成功后，系统会将被停止的 `webservice` 组件运行态写为 `Stopped`，并把 `ready_replicas` 置为 `0`。`store` 组件不会被 stop 停止，仍可保持 `Running`。随后可以通过批量状态接口查询应用聚合状态：

```bash
curl -sS http://127.0.0.1:8000/api/v1/applications/components/status \
  -H 'Content-Type: application/json' \
  -d '{"appIds":["app-123"]}'
```

当该应用没有更高优先级的组件状态，并且 `webservice` 组件均已停止时，即使 `store` 组件仍为 `Running`，响应也会显示 `stopped`：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "results": [
      {
        "appId": "app-123",
        "status": "stopped"
      }
    ]
  }
}
```



输出示例：

```text
APP-ID   STATUS    ERROR
app-123  stopped   -
```

## 单条失败示例

部分应用查询失败时，失败项写入 `error`，其他应用继续返回 `status`。

```json
{
  "code": 0,
  "message": "",
  "data": {
    "results": [
      {
        "appId": "app-123",
        "status": "running"
      },
      {
        "appId": "missing-app",
        "error": "application not found"
      },
      {
        "appId": "",
        "error": "appId is required"
      },
      {
        "appId": "app-timeout",
        "error": "store timeout"
      }
    ]
  }
}
```

## 参数错误示例

当 Body 不是 JSON 对象、缺少 `appIds` 字段，或 `appIds` 为空数组时，接口返回全局参数错误。

请求示例：

```json
{
  "appIds": []
}
```

响应示例：

```json
{
  "code": 10000,
  "message": "application config does not comply with OAM specification",
  "data": null
}
```

说明：

- 参数错误会使用 HTTP 400。
- 单个 `appIds[]` 为空字符串不是全局参数错误，而是对应 `results[]` 的单条错误。
