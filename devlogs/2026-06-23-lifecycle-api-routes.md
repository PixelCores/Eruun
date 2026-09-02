# Lifecycle API Route Simplification

## 背景与需求

应用生命周期 API 中存在两个语义不清的路径：`POST /applications/:appID/delete` 把资源删除表达成了动作接口，`POST /applications/:appID/cancelall` 没有体现取消对象是应用下的 workflow tasks。

本次目标是收敛这两个路径：删除应用使用标准的 HTTP DELETE；批量取消 workflow tasks 放到 app-scoped workflow tasks 路径下。`start/stop/restart` 保持已有路径，不额外引入 `actions` 子资源。

## 影响范围

- API: `DELETE /api/v1/applications/:appID` 成为删除应用入口；`POST /api/v1/applications/:appID/workflow/tasks/cancel-all` 成为批量取消应用 workflow tasks 的入口。
- Domain: 业务执行流程不变，响应结构不新增字段。
- DB: 无 schema 变化。
- Cache: 无变化。
- K8s: 无资源生成或执行语义变化。
- Workflow: 任务取消逻辑不变。

## 技术选型与取舍

- 不新增 `/actions/start|stop|restart`。这些路径不会带来新的资源边界或权限边界，反而增加无意义实体。
- 不保留 `/delete` 与 `/cancelall` 兼容路由。本次 PR 明确切换到最终 API 形态。
- 不新增 `partialSuccess` 响应字段。已有 `warnings`、`activeTaskIds`、`failedResources` 明细足以表达非完全成功状态。

## 实现摘要

- 删除 `POST /api/v1/applications/:appID/delete`，保留 `DELETE /api/v1/applications/:appID`。
- 删除 `POST /api/v1/applications/:appID/cancelall`，保留 `POST /api/v1/applications/:appID/workflow/tasks/cancel-all`。
- `POST /api/v1/applications/:appID/start|stop|restart` 保持不变，不注册 `/actions/...`。
- 移除 `partialSuccess,omitempty` 相关 DTO、赋值逻辑、测试断言和文档说明。

## 测试与验收

计划执行：

```bash
git diff --check
```

## 风险与后续

旧 `/delete` 与 `/cancelall` 不再注册，旧客户端需要切到新的删除和 cancel-all 路径。`start/stop/restart` 不变。
