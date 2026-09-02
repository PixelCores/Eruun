# Version Update Task Callback

## 背景与需求

版本更新接口需要支持不同更新场景使用不同终态通知地址。现有 callback 只有 App 和 Workflow 两层默认配置，如果在版本更新时临时覆盖 Workflow callback，会影响后续执行和并发更新场景。

## 影响范围

- API: `POST /api/v1/applications/:appID/version` 新增可选 `callback`，响应返回本次关联的 `workflowId`
- Domain: 版本更新 auto-exec 创建 workflow task 时保存 task 级 callback 快照；no-op operation task 可保存请求中已验证的 `workflowId`
- DB: `eruun_workflow_queue` 新增 nullable JSON callback 字段
- Workflow: 终态 callback 读取优先级调整为 task > workflow > app

## 技术选型与取舍

最终选择 task 级 callback 快照。它只影响本次版本更新产生的 workflow task，不污染 App 或 Workflow 默认配置，也避免并发版本更新互相覆盖 callback。

放弃直接覆盖 `workflow.callback`，因为它会改变长期默认行为。放弃新增独立 callback 配置表，因为当前只需要随 task 生命周期保存一次性配置，新增实体会扩大查询和清理复杂度。

## 实现摘要

`UpdateVersionRequest.callback` 复用现有 `WorkflowCallback` 结构。`autoExec=true` 且有实际组件变更、会创建 workflow task 时，系统规范化并校验 callback URL，然后写入 `WorkflowQueue.Callback`。普通终态回调和审批取消终态回调都先读取 task callback，再回退到 workflow/app callback。

无实际组件变更但传入有效 callback 时，系统创建已完成的 update operation task 并触发一次 success callback；如果请求同时提供了有效 `workflowId`，该 ID 会写入 operation task，并出现在 `/version` 响应和 callback payload 中。`autoExec=false` 会忽略 callback，不校验、不触发，也不会回传或写入请求中的 `workflowId`。

## 测试与验收

- API 接收 `/version` callback 字段
- auto-exec 持久化 task callback 且不修改 App/Workflow callback
- autoExec=false 忽略非法 callback
- auto-exec 下非法 callback 回滚版本、组件和 task
- no-op callback 在请求提供有效 `workflowId` 时通过响应、operation task 和 callback payload 回传
- workflow controller 与审批取消路径均验证 task callback 优先级

## 风险与后续

当前回调 payload 复用现有 workflow payload，不包含版本更新差异摘要。若后续通知方需要直接获得 updated/added/removed 组件，可在 callback payload 上新增兼容字段。
