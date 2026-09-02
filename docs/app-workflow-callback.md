# App 与 Workflow Callback 契约

> 状态：Current。本文说明创建 App、创建/更新 Workflow 与执行终态回调时的 callback 优先级。

## 请求形态

App 创建接口推荐使用 `components` 字段传入组件列表；历史字段 `component` 仍兼容接收，但不能和 `components` 同时出现。

App 创建接口继续兼容旧的 workflow 数组：

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
      "components": ["web"]
    }
  ]
}
```

如果需要在自定义 workflow 上声明独立 callback，可以使用新的 workflow 对象形态：

```json
{
  "name": "demo",
  "callback": {
    "success": "https://example.com/app/success"
  },
  "workflow": {
    "callback": {
      "success": "https://example.com/workflow/success"
    },
    "steps": [
      {
        "name": "deploy-web",
        "components": ["web"]
      }
    ]
  }
}
```

同一个 workflow 对象还可以声明 `failurePolicy`；它与 `callback` 同级，只控制部署失败后的清理策略，不改变 callback 优先级。失败清理策略详见 `workflow-failure-policy.md`。

更新已有 workflow 时，推荐使用 `steps` 传入步骤列表；历史字段 `workflow` 仍兼容接收。读接口返回 `steps[].workflowType`，更新接口也兼容接收该旧字段；读接口中的 `steps[].properties[]` / `subSteps[].properties[]` 数组也可以直接作为更新请求输入。新请求示例建议使用 `jobType`：

```json
{
  "name": "deploy-flow",
  "workflowType": "workflow",
  "steps": [
    {
      "name": "deploy-web",
      "jobType": "deploy",
      "components": ["web"]
    }
  ]
}
```

## 优先级

- 创建 App 时如果 `workflow` 为空或未提供，服务端生成的默认 workflow 使用根级 `callback`。
- 创建 App 时如果 `workflow.steps` 非空且 `workflow.callback` 非空，则 workflow 使用 `workflow.callback`，根级 `callback` 不参与校验和生效。
- 通过带 `ID` 的 `POST /api/v1/applications` 更新 App 时，如果根级 `callback` 非空或为 `{}`，服务端会把它写入 App，并覆盖该 App 下全部 workflow callback；`{}` 表示清空。
- `PUT /api/v1/applications/:appID/workflow` 仍只更新目标 workflow 的 callback，不更新 App callback。
- `POST /api/v1/applications/:appID/version` 可提供本次版本更新 task 级 `callback`；它只覆盖本次自动执行产生的 workflow task，不写入 App 或 Workflow。
- `POST /api/v1/applications/:appID/start|stop|restart` 可提供本次生命周期操作 task 级 `callback`；它只覆盖本次 operation task，不写入 App 或 Workflow。

## 执行时回调

Workflow 终态回调优先读取 `task.callback`，再读取 `workflow.callback`。如果历史数据或手工清理导致目标 workflow 没有 callback，则回退读取 `app.callback`。

版本更新 task 级 callback 在 `autoExec=true` 且创建 workflow task 时挂到 workflow task；无实际组件变更/资源动作但 `callback` 非空时，挂到本次已完成的 update operation task 并发送一次 `success` 回调；`autoExec=false` 时会被忽略。

生命周期操作 task 级 callback 只在 start/stop/restart 请求体显式提供 `callback` 时生效。空 body 或 `{}` 不触发回调，也不会回退 App callback。成功操作发送 `success` 事件；包含 `failedResources` 的部分失败 operation task 发送 `failure` 事件。callback payload 复用现有字段，`workflowType` 为 `start`、`stop` 或 `restart`，`workflowId` 为空。显式 task callback 需要先成功持久化对应 task；如果 operation task 创建失败，请求会返回错误且不会尝试发送 callback。

Callback URL 仍受 `urlSecurityPolicy` 约束；私网、回环和重定向目标按现有出站 URL 安全策略校验。Callback 只跟随同 origin 重定向：scheme、host 或有效端口发生变化时请求直接失败，配置的自定义 Header 不会发送到跨 origin 目标。

示例：

- `examples/workflow-callback/create-app-default-callback-request.json`
- `examples/workflow-callback/create-app-workflow-callback-override-request.json`
- `examples/workflow-callback/update-app-callback-overwrite-request.json`
