# 应用与组件状态 API

> 状态：Current。本文是应用运行态的主契约文档，覆盖单应用聚合状态、组件状态明细、批量应用聚合状态，以及状态优先级和状态扭转规则。

## 概述

Eruun 的应用状态分为两层：

- 应用聚合状态：面向页面和外部调用方，回答“这个 APP 当前整体是什么状态”。
- 组件运行态明细：面向排障和明细展示，回答“每个 component 当前是什么状态、ready 副本数是多少、最近异常是什么”。

当前可用接口如下：

| 接口 | 用途 | 返回状态格式 |
| --- | --- | --- |
| `GET /api/v1/applications/:appID/status` | 查询单个 APP 的应用聚合状态 | 小写聚合状态，如 `running`、`starting` |
| `GET /api/v1/applications/:appID/components/status` | 查询单个 APP 下所有组件运行态明细 | 组件原始状态，如 `Running`、`Starting` |
| `POST /api/v1/applications/components/status` | 批量查询一个或多个 APP 的应用聚合状态 | 小写聚合状态，如 `running`、`starting` |

`adopted` 组件保存内部 source workload identity。组件日志、容器列表、文件下载/上传和 exec 不按 Eruun 生成名称或管理标签猜测 Pod，而是校验 source owner UID 链：Deployment 通过 `Deployment -> ReplicaSet -> Pod`，StatefulSet/DaemonSet/Job 直接匹配 Pod owner，CronJob 通过 `CronJob -> Job -> Pod`。同名但 owner UID 不匹配的 Pod 不会进入查询结果。

状态事实源来自 `eruun_app_components.status`、`eruun_app_components.ready_replicas`、`eruun_app_components.last_abnormal` 和组件 `update_time`。上述三个状态接口都会直接读取组件 repository 的最新持久化快照，不读取也不回填普通 Components API 使用的 24 小时组件缓存；读取后仍沿用既有的 ready 副本纠正规则。应用聚合状态基于同一组组件快照计算，但会应用聚合优先级、服务组件优先规则、`/version` 活跃任务覆盖规则，以及 3 分钟 transient failed 平滑规则。

生命周期操作范围和应用状态语义不是同一层概念：`start` / `stop` 只控制 `webservice` 对应的 Deployment；应用聚合状态表达服务可用性。若应用包含生命周期可控的 `webservice`，普通可用性状态（`pending`、`running`、`stopped`、`not_deploy`、`unknown`）优先由这些 managed webservice 决定。未配置 `share` 和 `share=force` 的组件属于 managed；`share=default` / `share=ignore` 不参与 managed availability，避免复用的 proxy 等公共 workload 覆盖本应用的启停或就绪状态。`store`、共享组件等任意组件的 `failed`、`deploying`、`updating`、`restarting`、`starting`、`cleaning` 仍会作为全局高优先级状态影响应用聚合结果。若应用只有共享 webservice，则回退按这些共享组件的真实状态聚合，不把“存在共享资源”伪造为 `running`。

## 单应用聚合状态

- 方法：`GET`
- 路径：`/api/v1/applications/:appID/status`

成功响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "status": "starting"
  }
}
```

`data.status` 返回应用聚合状态，状态值统一为小写。该接口适合前端首页、列表页、详情页顶部状态、PaaS runtime proxy 等只需要 APP 整体状态的场景。

## 组件状态明细

- 方法：`GET`
- 路径：`/api/v1/applications/:appID/components/status`

成功响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "components": [
      {
        "name": "web",
        "namespace": "default",
        "type": "webservice",
        "status": "Starting",
        "replicas": 1,
        "readyReplicas": 0,
        "lastAbnormal": ""
      },
      {
        "name": "mysql",
        "namespace": "default",
        "type": "store",
        "status": "Running",
        "replicas": 1,
        "readyReplicas": 1,
        "lastAbnormal": ""
      }
    ]
  }
}
```

组件明细接口直接读取 repository 中最新保存的组件运行态，不会把 `Starting` 转成小写，也不会应用应用聚合状态的 3 分钟 `Failed` 平滑。它沿用普通组件读取的既有纠正规则：Pod 型组件处于 `Pending`、`Updating` 或 `Deploying`，且 `ready_replicas >= replicas > 0` 时，响应按 `Running` 展示。排查短暂异常、Pod 异常、ready 副本数、`lastAbnormal` 时应使用该接口。

`GET /api/v1/workflow/:appID/components/status` 不再注册；组件运行态属于应用资源，不作为 workflow 顶层接口暴露。

## 批量应用聚合状态

- 方法：`POST`
- 路径：`/api/v1/applications/components/status`

请求体：

```json
{
  "appIds": [
    "app-123",
    "app-456"
  ]
}
```

成功响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "results": [
      {
        "appId": "app-123",
        "status": "starting"
      },
      {
        "appId": "missing-app",
        "error": "application not found"
      }
    ]
  }
}
```


## 应用聚合状态全集

| 聚合状态 | 典型来源组件状态 | 含义 |
| --- | --- | --- |
| `failed` | `Failed` | 至少一个有效组件失败。 |
| `deploying` | `Deploying` | 初次部署 workflow 已把目标组件标记为部署中，且没有失败组件。 |
| `updating` | `Updating` 或活跃 `/version` workflow task | 至少一个组件处于更新中，或即时 `/version` 真实 workflow task 仍活跃。 |
| `restarting` | `Restarting` | 至少一个组件处于重启中，且没有更高优先级状态。 |
| `starting` | `Starting` | 至少一个组件由 `/start` 从 `Stopped` 恢复后正在启动，且没有更高优先级状态。 |
| `cleaning` | `Cleaning` | 至少一个 Pod 型组件处于资源清理中，且没有更高优先级状态。 |
| `pending` | `Pending` | managed 服务组件等待调度或副本未就绪，且没有更高优先级状态；没有 managed `webservice` 时回退到现有服务组件或全组件。 |
| `running` | `Running` | managed 服务组件运行中，且没有更高优先级状态；没有 managed `webservice` 时回退到现有服务组件或全组件。 |
| `stopped` | `Stopped` | managed 服务组件已停止，且没有更高优先级状态；没有 managed `webservice` 时回退到现有服务组件或全组件。 |
| `not_deploy` | `Not Deploy`、空组件状态、无组件 | 应用没有可用服务部署状态，或组件尚未部署。 |
| `unknown` | `Unknown`、无法识别的组件状态 | 服务组件状态无法判断，且没有更高优先级状态；没有 `webservice` 组件时按全组件聚合。 |

组件状态在 DB 中使用首字母大写形式，例如 `Running`、`Pending`、`Starting`。应用聚合状态统一输出小写形式，例如 `running`、`pending`、`starting`；`Not Deploy` 在聚合结果中输出为 `not_deploy`。

## 聚合优先级

应用聚合优先级如下：

```text
failed > deploying > updating > restarting > starting > cleaning > pending > running > stopped > not_deploy > unknown
```

核心规则：

- `failed` 最高：一个应用中只要存在有效 `Failed` 组件，聚合结果就是 `failed`。
- `deploying` 高于 `updating`：初次部署中的状态优先表达为 `deploying`，避免被版本更新语义混淆。
- `starting` 只表达 `/start` 恢复：它排在 `restarting` 后、`cleaning` 前，不用于初次部署或普通调度等待。
- 有 managed `webservice` 时，普通可用性状态以 managed `webservice` 为主：例如 managed `webservice=Stopped`、`store=Running`、共享 proxy=`Running` 时，应用聚合为 `stopped`。
- `share=default` / `share=ignore` 的 webservice 不参与 managed availability；例如 managed backend/frontend/socket=`Running`、共享 proxy=`Pending` 时，应用聚合为 `running`。`share=force` 与未配置 `share` 一样参与 managed availability。
- 只有共享 webservice 时，按共享组件的现有状态聚合；共享资源存在本身不等价于 Ready。
- 高优先级全局状态不受 `webservice` 可用性规则限制：例如 `store=Failed`、`webservice=Stopped` 时，应用仍聚合为 `failed`；`store=Restarting`、`webservice=Running` 时，应用聚合为 `restarting`。
- 没有 `webservice` 组件时，按所有组件状态聚合。

## 状态扭转

### create-and-exec 初次部署

`POST /api/v1/applications/create-and-exec` 创建应用并立即排队初次部署 workflow 时，Eruun 会把本次 workflow 中部署类 component step/substep 覆盖、仍处于空状态或 `Not Deploy` 且具备部署终态回写路径的组件标记为 `Deploying`。

典型扭转：

```text
空状态 / Not Deploy -> Deploying -> Pending -> Running
空状态 / Not Deploy -> Deploying -> Pending -> Failed
```

说明：

- `Deploying` 用于表达初次部署中。
- workflow job 开始执行具体资源调和时，目标组件可进入 `Pending`。
- Informer 后续根据副本就绪情况推进到 `Running`，或根据异常推进到 `Failed`。
- 未来时间的延迟初次部署 task 不会提前覆盖状态；终态 task 不会继续覆盖状态。

### /version 即时更新

`POST /api/v1/applications/:appID/version` 的即时真实 workflow task 会让目标组件或应用聚合结果表达为 `updating`。

典型扭转：

```text
Running / Pending / Stopped / Not Deploy / Unknown -> Updating -> Pending -> Running
Running / Pending / Stopped / Not Deploy / Unknown -> Updating -> Pending -> Failed
```

说明：

- 更新目标组件会被标记为 `Updating`，但当前为 `Cleaning` 的组件不会被覆盖。
- 当即时 `/version` 已创建真实 workflow task，且 task 仍处于 `waiting`、`queued`、`running`、`pending`、`prepare`、`wait_for_approval` 等活跃状态时，应用聚合状态可被提升为 `updating`。
- 该提升只作用于原本会落到 `running`、`pending`、`stopped`、`not_deploy`、`unknown` 的聚合结果。
- `failed`、`deploying`、`updating`、`restarting`、`starting`、`cleaning` 等更高或更具体状态保持原样。
- 未来时间的延迟 `/version` task 不会提前覆盖状态。

### /stop 停止服务组件

`POST /api/v1/applications/:appID/stop` 只控制 `webservice` 对应的 Deployment，把副本缩到 0。

典型扭转：

```text
Running / Pending / Failed / Unknown -> Stopped
```

说明：

- 成功停止的 `webservice` 组件会写入 `Stopped`，并将 `ready_replicas` 置为 `0`。
- `store` 组件不会因 `/stop` 自动停止，可继续保持 `Running`。
- 有 `webservice` 组件时，`webservice=Stopped`、`store=Running` 的应用聚合结果是 `stopped`。
- `share=default` / `share=ignore` / 未知 share 策略的组件会被跳过，不参与缩容，也不参与 managed availability；`share=force` 仍按普通组件处理。跳过目标通过 `skippedResources` 返回。

### /start 从停止态恢复

`POST /api/v1/applications/:appID/start` 只恢复已处于 `Stopped` 的 `webservice` 组件。

典型扭转：

```text
Stopped -> Starting -> Running
Stopped -> Starting -> Failed
```

说明：

- 非 `Stopped` 组件会被跳过，不会被改成 `Starting`。
- `share=default` / `share=ignore` / 未知 share 策略的组件不会扩容或改写状态，并通过 `skippedResources` 返回；`share=force` 仍按普通组件处理。
- 成功扩容后，组件写入 `Starting`，`ready_replicas` 置为 `0`，`last_abnormal` 清空。
- Informer 同步时，当前 `Starting` 不会被 `Pending` 或 `Unknown` 立即覆盖，避免刚扩容时页面回退成普通等待态。
- `Running` 或 `Failed` 是结束 `Starting` 的终态同步信号。
- `starting` 不用于初次部署；初次部署使用 `deploying`，普通等待使用 `pending`。

### /restart 重启工作负载

`POST /api/v1/applications/:appID/restart` 对非 `Stopped` 的 `webservice` 和 `store` 工作负载触发重启。

典型扭转：

```text
Running / Pending / Failed / Unknown -> Restarting -> Running
Running / Pending / Failed / Unknown -> Restarting -> Failed
```

说明：

- 当前为 `Stopped` 的组件会被跳过，不会被重启。
- `share=default` / `share=ignore` / 未知 share 策略的组件不会写 restart annotation 或运行态字段，并通过 `skippedResources` 返回；`share=force` 仍按普通组件处理。
- 成功 patch 的组件会写入 `Restarting`。
- 后续由 Informer 同步推进到 `Running` 或 `Failed`。
- 任意组件处于 `Restarting` 时，只要没有更高优先级状态，应用聚合结果就是 `restarting`。

### cleanup 资源清理

资源清理会把 Pod 型组件标记为 `Cleaning`，用于表达资源删除尚未完成。

典型扭转：

```text
Running / Pending / Failed / Unknown -> Cleaning -> Not Deploy
```

说明：

- Pod 型组件开始清理时写入 `Cleaning`，并将 `ready_replicas` 置为 `0`、`last_abnormal` 清空。
- 清理过程中普通 Informer 状态不会覆盖 `Cleaning`。
- 当 Informer 或清理检查确认副本/Pod 消失后，组件转为 `Not Deploy`。

### Informer 同步

Informer 是组件运行态持续更新的来源之一。

常规推导：

```text
desired > 0 且 ready >= desired -> Running
desired > 0 且 ready < desired  -> Pending
desired = 0 且 ready = 0        -> Failed
```

保护规则：

- 当前为 `Not Deploy` 的组件不被普通 Informer 状态覆盖。
- 当前为 `Stopped` 的组件不被普通 Informer 状态覆盖。
- 当前为 `Cleaning` 的组件只在副本数为 0 时转为 `Not Deploy`，否则保持 `Cleaning`。
- 当前为 `Starting` 或 `Deploying` 的组件不会被 `Pending`、`Unknown` 立即覆盖，但会被 `Running`、`Failed` 结束。

一致性规则：

- 同一 `(appID, componentID)` 的状态事件在一个串行 lane 中处理；等待执行的多个中间快照只保留最新值，不同组件仍可并行。
- Informer Manager 创建的事件处理器会捕获当前 runtime generation。重建或停止会形成进程内执行栅栏：等待已经开始的旧处理器/状态回调退出，并丢弃尚未开始的旧 generation 更新，不能在栅栏返回后再启动旧回写。
- Informer 写入使用读取快照的 `status + ready_replicas + last_abnormal` 做 compare-and-set。`update_time` 是组件整行的通用更新时间，不参与运行态 CAS；仅修改 image、traits 等配置并推进 `update_time` 时，不会误丢弃合法的 Informer 状态。若显式 `stop`、`start`、`restart`、`version` 或其他权威生命周期写入已改变运行态三元组，旧 Informer 事件发生冲突并直接丢弃，不重新读取后覆盖。
- `Cleaning -> Not Deploy` 也遵守同一 compare-and-set 规则。
- 普通组件缓存会在状态同步完成或判定为冲突、终态跳过后统一失效；状态接口本身仍直接读取 repository。

内部 generation 只约束当前进程中的 Informer runtime，不是跨实例分布式 fencing token；多实例 leader 交接期间的写入仍以持久化运行态快照 CAS 为最终保护。若两个写入的运行态三元组完全相同，现有模型无法区分其语义代次；严格跨实例全序需要独立、原子递增的 runtime revision。

### transient failed 平滑

应用聚合状态会对 Pod 型组件的新近 `Failed` 做 3 分钟宽限，避免 init container 短暂异常或重新调度导致页面在 `failed` 与恢复态之间闪烁。

规则：

- 当组件 `update_time` 距查询时间不超过 3 分钟，且组件类型使用 Pod：
  - 若 `replicas > 0` 且 `ready_replicas >= replicas`，该组件在应用聚合中按 `Running` 参与计算。
  - 否则按 `Pending` 参与计算。
- 超过 3 分钟仍为 `Failed`、非 Pod 型组件 `Failed`、或 `update_time` 为空时，继续按 `Failed` 参与聚合。
- 该平滑只影响应用聚合状态；组件明细接口仍返回原始 `Failed` 和 `lastAbnormal`。

## 响应示例

单应用聚合状态：

```bash
curl -sS http://127.0.0.1:8000/api/v1/applications/app-123/status
```

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "status": "starting"
  }
}
```

组件明细状态：

```bash
curl -sS http://127.0.0.1:8000/api/v1/applications/app-123/components/status
```

```json
{
  "code": 0,
  "message": "",
  "data": {
    "appId": "app-123",
    "components": [
      {
        "name": "web",
        "namespace": "default",
        "type": "webservice",
        "status": "Starting",
        "replicas": 1,
        "readyReplicas": 0
      }
    ]
  }
}
```

批量应用聚合状态：

```bash
curl -sS http://127.0.0.1:8000/api/v1/applications/components/status \
  -H 'Content-Type: application/json' \
  -d '{"appIds":["app-123","missing-app"]}'
```

```json
{
  "code": 0,
  "message": "",
  "data": {
    "results": [
      {
        "appId": "app-123",
        "status": "starting"
      },
      {
        "appId": "missing-app",
        "error": "application not found"
      }
    ]
  }
}
```
