# 工作流审批暂停/继续说明

> 状态：Current。本文说明当前 approval step、暂停状态和继续/取消入口。

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
4. `approval.timeoutSeconds` 可选，默认 `0`（不超时，持续等待人工操作）；大于 `0` 时超时由 Leader 按数据库时钟判定，见下文“审批超时”。

## 运行时行为

1. 命中审批步骤后，任务状态切换为 `wait_for_approval`。
2. 引擎用 owner fencing CAS（`status=running` 且 `currentStep/runGeneration/runToken/workerId` 匹配）持久化检查点：`currentStep`、`approvalPending`、`pendingApprovalStep`、`approvalDeadline`。
3. 引擎主动发送审批通知 webhook（事件名 `approval_required`）。
4. Worker 写完检查点即结束本次执行，不再续租；等待期间任务不会进入终态，也不会触发终态 callback。

## 审批超时

`timeoutSeconds>0` 时，检查点写入前先读取数据库时钟，`approval_deadline = DB now + timeoutSeconds` 与检查点在同一条 CAS 中落库；`timeoutSeconds=0` 时该列写为 `NULL`。数据库时钟不可用时检查点不写入、执行直接返回错误，任务保持 `running` 并由 Scheduler lease reaper 在租约过期后恢复为 `waiting`，下次执行重新写检查点。

超时判定不依赖暂停任务的 Worker 进程：Leader 的 lease reaper 每个周期（`--workflow-lease-reaper-interval`，默认 10s，批量上限沿用 `LeaseReaperBatchSize`）读取数据库时钟，按索引 `(status, approval_deadline)` 扫描 `status=wait_for_approval AND approval_deadline < DB now` 的任务，按截止时间升序逐条 CAS：

| 条件（全部匹配） | 更新 |
|---|---|
| `status=wait_for_approval`、`approval_pending=true`、`current_step`、`pending_approval_step`、`run_generation`、`approval_deadline`（等于扫描时读到的值） | `status=timeout`、`approval_pending=false`、`pending_approval_step=""`、清空 `run_token/worker_id/heartbeat_at/lease_expires_at`、`scheduling_reason="approval deadline exceeded"` |

CAS 成功后，Leader 把版本更新预创建且未开始的清理记录置为 `timeout`，再异步发送终态 `timeout` callback（reason 为 `approval step "<name>" timed out`）。因此：

1. Worker 重启、滚动升级或 OOM 不影响超时触发；实际触发时间在截止时间后的一个 reaper 周期内。
2. 审批 `continue/cancel` 与超时竞争同一组检查点条件，只有一方 CAS 成功；输掉的一方不改变任务状态（审批接口返回任务不在审批中）。
3. 任务继续后再次暂停到后续审批步骤（即使步骤名相同）会写入新的 `currentStep` 和截止时间，旧扫描快照无法命中。
4. 终态 callback 与其他终态回调一样单次投递：Leader 在 CAS 提交后、callback 发出前退出时，状态仍正确收敛为 `timeout`，但该次 callback 不会补发。

`approval_deadline` 只在 `wait_for_approval` 期间有意义；每次写审批检查点时覆盖，其他状态迁移不清空。

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
