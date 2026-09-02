# 工作流审批暂停/继续说明

## 背景

为了支持发布前人工确认，工作流新增 `approval` 类型步骤。执行到该步骤时，任务进入 `wait_for_approval`，等待用户选择继续或取消。

## 工作流定义

在 `workflow` 数组中新增审批步骤：

```json
{
  "name": "manual-approval",
  "stepType": "approval",
  "mode": "StepByStep",
  "approval": {
    "notifyUrl": "https://example.com/workflow/approval",
    "message": "请确认是否继续发布",
    "method": "POST",
    "timeoutSeconds": 0
  }
}
```

约束：

1. `stepType=approval` 时，不能再配置 `components/properties/subSteps`。
2. `approval.notifyUrl` 必填，且必须是 `http/https`。
3. `approval.method` 可选，支持 `GET/POST/PUT/DELETE`，默认 `POST`。
4. `approval.timeoutSeconds` 可选，默认 `0`（不超时，持续等待人工操作）。

## 运行时行为

1. 命中审批步骤后，任务状态切换为 `wait_for_approval`。
2. 引擎会持久化检查点：`currentStep`、`approvalPending`、`pendingApprovalStep`。
3. 引擎主动发送审批通知 webhook（事件名 `approval_required`）。
4. 任务不会进入终态，也不会触发终态 callback。

## 审批接口

接口：`POST /api/v1/workflow/tasks/:taskID/approval`

请求体：

```json
{
  "action": "continue",
  "user": "approver",
  "reason": "approved"
}
```

`action` 支持：

1. `continue`：任务状态改为 `waiting`，并从审批步骤后的下一步继续。
2. `cancel`：任务进入 `cancelled`，复用现有取消逻辑。
3. 任务审批门禁以检查点为准：`approvalPending=true` 且状态为 `wait_for_approval` 或 `waiting` 时均可审批（覆盖服务重启后任务重排为 `waiting` 的窗口）。

响应体：

```json
{
  "taskId": "task-approve-1",
  "action": "continue",
  "status": "waiting"
}
```

## 恢复语义

服务重启后，如果审批任务被重新调度，会根据持久化检查点回到审批位置，不会跳过审批步骤。
为避免审批暂停期间工作流定义被修改导致步骤索引漂移，存在运行中/待审批任务时，工作流更新接口会返回 `409`（`workflow task is running`）。
