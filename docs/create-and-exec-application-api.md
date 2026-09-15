# 创建并执行应用 API

> 状态：Current。当前路由为 `POST /api/v1/applications/create-and-exec`。

## 背景

新增组合接口用于一次请求完成两个动作：

1. 创建应用（等价于 `POST /api/v1/applications`）
2. 执行应用工作流（等价于 `POST /api/v1/applications/:appID/workflow/exec`）

接口路径：

`POST /api/v1/applications/create-and-exec`

## 请求结构

请求体基于 `CreateApplicationsRequest`，新增两个可选字段：

- `workflowId`: 指定执行的工作流 ID
- `executeAt`: 延迟执行时间（Unix 秒）

组件列表只使用 `components`。根级 `component` 会作为未知字段拒绝。

因为该接口继承 `CreateApplicationsRequest`，工作流步骤固定使用根级数组 `workflow: [...]`，工作流配置使用同级的 `failurePolicy` 和 `callback`。根级 `steps` 与 `workflow: {"steps": [...]}` 均不接受。完整规范见 [Canonical JSON Profile](canonical-json-profile.md)，失败清理策略详见 `workflow-failure-policy.md`。

当未提供 `workflowId` 时，默认使用创建接口返回的 `workflowId`。

调用方可提供 `Idempotency-Key` header。键必须为 1 到 128 个字母、数字、点、下划线、冒号或连字符。对相同 workspace、Application、键、Workflow 和等价 `executeAt` 的重试，服务端复用第一次创建的 workflow task；同一个键改用于不同 Workflow 或 `executeAt` 时执行阶段冲突。该接口仍会先按现有声明式 upsert 语义创建或刷新 Application；幂等边界是 workflow task 入队。

## Namespace 生命周期

应用使用 `X-Eruun-Workspace-ID` 选中的空间（省略使用个人空间），`workspaceID` 为服务端必填归属。namespace 由空间确定；请求填写不一致 namespace 时拒绝。注册和保存应用不创建 namespace，首次实际部署才初始化安全基线。应用删除不删除 namespace；只有所有者可通过空间接口删除空团队。详见 [账号与空间](account-auth-workspaces.md)。

示例请求见：

- `examples/workflow-schedule/create-and-exec-application-request.json`

## 响应结构

返回体：

- `application`: 创建后的应用信息（`ApplicationBase`），资源摘要字段位于 `application.resources`；当本次创建请求的第一个主组件包含 `traits.resources` 时会同步返回对应 request、limit 与 replicas
- `workflowId`: 实际尝试执行的工作流 ID
- `taskId`: 执行成功时返回的任务 ID
- `execStatus`: `queued` 或 `failed`
- `execError`: 执行失败原因（仅 `execStatus=failed` 时返回）
- `allowedActions`: 当前任务可执行的结构化 HTTP 动作；普通排队或运行任务通常返回 `cancel`，无可执行动作时返回 `[]`

示例响应见：

- `examples/workflow-schedule/create-and-exec-application-response.json`

## 失败语义

- 创建失败：返回业务错误（与 `POST /applications` 一致）。
- 当请求携带既有应用 ID，或 template key 命中既有应用时，创建阶段实际是受 app-scoped 锁保护的整体刷新。如果该应用仍有未完成的 cleanup v2/v3 StatefulSet 迁移，刷新会在替换组件或 workflow 前返回 `400/10000`，不会改变组件 numeric ID，也不会继续调用 workflow 执行。
- 创建成功但执行失败：接口整体返回成功，`execStatus=failed`，并在 `execError` 中返回失败原因。
- 同一幂等作用域的键与不同 `executeAt` 冲突：workflow 执行阶段返回幂等冲突；Application 创建/刷新阶段已按上述 upsert 语义完成。

该设计避免“请求失败但应用已创建”的歧义，便于调用方进行补偿或重试执行。

## 状态同步

创建成功且 workflow 立即排队执行后，Eruun 会把本次 workflow 中 `workflowType` 为空或 `deploy` 的 component step/substep 覆盖、当前空状态或 `Not Deploy` 且具备部署终态回写路径的组件运行态标记为 `Deploying`，组件类型限于 `webservice`、`store`、`job`、`scheduledjob`、`config` 和 `secret`。未被本次 workflow 覆盖的组件、非部署步骤覆盖的组件、已有 `Running` / `Stopped` / `Cleaning` / `Updating` 等状态的组件以及 `cloudjob` 不会被标记。`GET /api/v1/applications/:appID/status` 和 `POST /api/v1/applications/components/status` 基于该组件态把应用聚合状态返回为 `deploying`，用于表达初次部署中。若请求使用未来时间的 `executeAt` 延迟执行，状态不会提前从 `not_deploy` 提升。

## 验证建议

```bash
go test ./pkg/apiserver/interfaces/api -run TestCreateAndExecApplications -count=1
```
