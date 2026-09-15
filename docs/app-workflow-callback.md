# App 与 Workflow Callback 契约

> 状态：Current。本文说明创建 App、创建/更新 Workflow 与执行终态回调时的 callback 优先级。

## 请求形态

App 创建接口只使用 `components` 字段传入组件列表，工作流步骤固定使用根级 `workflow` 数组：

```json
{
  "name": "demo",
  "components": [
    {
      "name": "web",
      "type": "webservice",
      "image": "nginx:latest",
      "replicas": 1,
      "properties": {},
      "traits": {}
    }
  ],
  "callback": {
    "success": "https://example.com/app/success",
    "failure": "https://example.com/app/failure"
  },
  "workflow": [
    {
      "name": "deploy-web",
      "components": [
        "web"
      ]
    }
  ]
}
```

Application 根级 `callback` 会同时作为显式 `workflow` 的 callback：

```json
{
  "name": "demo",
  "components": [
    {
      "name": "web",
      "type": "webservice",
      "image": "nginx:latest",
      "properties": {},
      "traits": {}
    }
  ],
  "callback": {
    "success": "https://example.com/workflow/success"
  },
  "workflow": [
    {
      "name": "deploy-web",
      "components": [
        "web"
      ]
    }
  ]
}
```

Application 根级还可以声明 `failurePolicy`；它只控制部署失败后的清理策略，不改变 callback 优先级。需要为某个已存在的 workflow 设置独立 callback 时，使用下方 Workflow 更新接口。失败清理策略详见 `workflow-failure-policy.md`。

更新已有 workflow 时只使用根级 `workflow` 传入步骤列表，并可在同级声明该 workflow 独立的 `callback`。读取 Workflow 时使用其 `spec` 字段即可直接编辑并重新提交；规范输出使用 `jobType` 和 `properties[]`：

```json
{
  "name": "deploy-flow",
  "workflowType": "workflow",
  "workflow": [
    {
      "name": "deploy-web",
      "jobType": "deploy",
      "components": [
        "web"
      ]
    }
  ]
}
```

## 优先级

- 创建 App 时如果 `workflow` 为空或未提供，服务端生成的默认 workflow 使用根级 `callback`。
- 创建 App 时如果根级 `workflow` 非空，显式 workflow 同样使用根级 `callback`。
- 通过带 `ID` 的 `POST /api/v1/applications` 更新 App 时，如果根级 `callback` 非空或为 `{}`，服务端会把它写入 App，并覆盖该 App 下全部 workflow callback；`{}` 表示清空。
- `GET /api/v1/applications/:appID/spec` 只有在全部已存 workflow callback 都与 App callback 一致时才返回根级 `callback`。存在独立 callback 或依赖 App fallback 的 workflow 时省略该字段，确保原样重提不会触发上述全量覆盖；调用方仍可显式加入根级 `callback` 请求统一覆盖。
- `PUT /api/v1/applications/:appID/workflow` 仍只更新目标 workflow 的 callback，不更新 App callback。
- `POST /api/v1/applications/:appID/version` 可提供本次版本更新 task 级 `callback`；它只覆盖本次自动执行产生的 workflow task，不写入 App 或 Workflow。
- `POST /api/v1/applications/:appID/start|stop|restart` 可提供本次生命周期操作 task 级 `callback`；它只覆盖本次 operation task，不写入 App 或 Workflow。

## 执行时回调

Workflow 终态回调优先读取 `task.callback`，再读取 `workflow.callback`。如果历史数据或手工清理导致目标 workflow 没有 callback，则回退读取 `app.callback`。

版本更新 task 级 callback 在 `autoExec=true` 且创建 workflow task 时挂到 workflow task；无实际组件变更/资源动作但 `callback` 非空时，挂到本次已完成的 update operation task 并发送一次 `success` 回调；`autoExec=false` 时会被忽略。

生命周期操作 task 级 callback 只在 start/stop/restart 请求体显式提供 `callback` 时生效。空 body 或 `{}` 不触发回调，也不会回退 App callback。成功操作发送 `success` 事件；包含 `failedResources` 的部分失败 operation task 发送 `failure` 事件。callback payload 复用现有字段，`workflowType` 为 `start`、`stop` 或 `restart`，`workflowId` 为空。显式 task callback 需要先成功持久化对应 task；如果 operation task 创建失败，请求会返回错误且不会尝试发送 callback。

Callback URL 仍受 `urlSecurityPolicy` 约束；私网、回环和重定向目标按现有出站 URL 安全策略校验。Callback 只跟随同 origin 重定向：scheme、host 或有效端口发生变化时请求直接失败，配置的自定义 Header 不会发送到跨 origin 目标。

Callback 保持单次投递语义。持久化的 callback Job 终态表示该次投递已有可审计结果；`failed`、`timeout` 等失败结果不会由运行时自动重试。取消恢复只会重放尚未形成持久化终态的 callback，并复用同一 `Idempotency-Key`，以便接收方在结果不确定时去重。

示例：

- `examples/workflow-callback/create-app-default-callback-request.json`
- `examples/workflow-callback/create-app-explicit-workflow-callback-request.json`
- `examples/workflow-callback/update-app-callback-overwrite-request.json`
