# Lifecycle Task Callback

## 背景与需求

`POST /api/v1/applications/:appID/start|stop|restart` 之前只支持空请求体。PaaS 等调用方需要在单次生命周期操作结束后接收回调，但该回调不应该写入 App 默认 callback，也不应该覆盖 Workflow callback。

## 影响范围

- API: start/stop/restart 请求体继续兼容空 body，同时支持可选 `callback`。
- Domain: 生命周期操作完成后仍记录 operation task；请求级 callback 挂到该 task。
- DB: 复用 `workflow_queue.callback`，无 schema 变化。
- Workflow: 复用现有终态 callback job 执行链路，不新增回调执行器。

## 技术取舍

- 不回退 App callback。生命周期请求未显式传 `callback` 时保持旧行为，避免旧调用突然触发外部回调。
- 不新增 lifecycle callback 实体。已有 `WorkflowQueue.Callback` 能表达 task-scoped callback，符合最小实体原则。
- 不改变响应结构。`taskId` 仍是调用方关联本次 operation task 和 callback payload 的字段。

## 实现摘要

- 新增 `ApplicationLifecycleRequest.callback`。
- handler 使用可空严格 JSON 绑定：空 body 合法，未知字段或 malformed JSON 返回应用配置错误。
- service 在 App 读取后、K8s 变更前校验 callback URL/method/header/timeout。
- start/stop/restart operation task 保存请求级 callback，并复用终态 callback 执行器发送 `success` 或 `failure`。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/interfaces/api ./pkg/apiserver/domain/service/application ./pkg/apiserver/domain/service/workflow ./pkg/apiserver/event/workflow/job
go test ./...
git diff --check
```
