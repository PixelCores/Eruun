# Database Reset Workflow

> 状态：Current。本文描述当前数据库重置接口、WorkflowQueue 执行模型和 Kubernetes 资源处理边界。

## Endpoint

```http
POST /api/v1/applications/:appID/database-reset
Content-Type: application/json
```

请求体：

```json
{
  "components": ["mysql", "redis"],
  "initSqlUrl": "https://files.example/game-1.0.8.sql"
}
```

- `components` 必填且至少 1 个元素。
- 每个元素是当前应用下的组件名。
- 组件必须存在且 `componentType=store`；未知组件或非 `store` 组件返回 `ErrApplicationConfig`。
- 接口不会默认重置全应用，调用方必须显式指定数据库组件。
- `initSqlUrl` 可选，表示可独立初始化空库的完整 SQL 快照。字段存在时必须是非空的绝对 HTTP/HTTPS URL；`null`、空字符串、纯空白、非字符串或非法 URL 都返回 `ErrApplicationConfig`，且不会创建 Workflow/Task。
- `initSqlUrl` 只应用到实际含有 initContainer `SQL_URL` 环境变量的 StatefulSet。Redis 等不含该环境变量的 `store` 组件仍按普通 PVC reset 执行，不会被写入 SQL URL。
- 兼容旧调用方：只有字段完全缺失时才保留 StatefulSet 当前 `SQL_URL`，并记录不包含 URL 内容的结构化警告。

成功响应仍使用统一 envelope，`data` 示例：

```json
{
  "appId": "app-123",
  "workflowId": "workflow-123",
  "taskId": "task-123",
  "databaseComponents": ["mysql", "redis"],
  "restartComponents": []
}
```

`restartComponents` 是为兼容现有调用方保留的响应字段，固定返回空数组。`database-reset` 不再收集或重启任何 `webservice`。

## Examples

可复制示例位于 [examples/database-reset-workflow](../examples/database-reset-workflow/README.md)：

- [01-database-reset-request.json](../examples/database-reset-workflow/01-database-reset-request.json)：重置 `mysql` 和 `redis` 两个 `store` 组件。
- [02-database-reset-response.json](../examples/database-reset-workflow/02-database-reset-response.json)：成功入队后的统一响应 envelope。
- [03-database-reset-invalid-component-response.json](../examples/database-reset-workflow/03-database-reset-invalid-component-response.json)：未知组件或非 `store` 组件错误响应。

## Execution Model

接口只负责校验和入队，不同步执行 Kubernetes 操作。

1. 读取应用和组件，校验请求组件都属于该应用且都是 `store`，并校验可选的 `initSqlUrl`。
2. 获取与手工执行、到期 schedule、版本更新自动执行共用的 app-scoped 分布式锁；锁覆盖本次读取、校验和 task 创建，并在长操作期间自动续期。
3. 在锁内的 datastore transaction 中校验应用没有活跃 workflow task，也没有 pending cleanup v2/v3 StatefulSet 迁移。
4. 在该 transaction 中创建一次性 Workflow 和 WorkflowQueue task。
5. Workflow worker 异步消费任务并执行 `database_reset` JobTask。

存在运行中任务时返回 `ErrWorkflowTaskRunning`；同一 App 的其他任务创建或互斥操作持有锁时返回 `409 / 10031`，且不会写入一次性 Workflow 或 WorkflowQueue task。该锁在创建事务提交后释放，不覆盖异步数据库重置执行；不同 App 不互相阻塞。

入队失败沿用统一错误 envelope：

| HTTP | Code | 场景 |
| --- | --- | --- |
| 400 | 10000 | 请求组件无效，或存在 pending cleanup v2/v3；后者必须先通过 `/version` 显式全量重建恢复 |
| 409 | 10031 | 同一应用的版本更新、workflow 入队、定时调度或数据库重置正在持有应用锁 |
| 503 | 10032 | 应用级分布式锁后端不可用 |
| 409 | 20007 | 应用已有活跃 workflow task |
| 409 | 20008 | 应用 workflow 正在取消且 Job 尚未收敛 |

一次性 Workflow 使用：

- `workflow_type=database_reset`
- step `workflowType=database_reset`
- step `mode=StepByStep`
- step `properties[].policies` 保存请求里的数据库组件名
- step `properties[].initSqlUrl` 显式保存初始化 SQL URL，保证异步调度、恢复和重试不丢失参数

Job builder 会把同一个 `database_reset` step 内的多个数据库组件聚合成一个 JobTask。每个顶层 step 或 substep 同时获得稳定的内部执行键；即使同一个 WorkflowQueue task 中存在多个名称均为 `database-reset` 的 JobTask，其恢复检查点也按执行键隔离，公开的 Job 名称和 `ServiceName` 保持不变。

## Kubernetes Behavior

JobTask 会先只读预检所有目标 StatefulSet、PVC reset 目标和 initContainer 环境变量。如果请求传了 `initSqlUrl`，但所有目标都不存在 `SQL_URL`，任务会在任何缩容、PVC 删除或组件运行状态修改前失败。MySQL 与 Redis 混合重置时，只要求至少有一个匹配目标，Redis 不参与 SQL URL 更新。

所有目标预检成功后，JobTask 会在修改组件状态或 Kubernetes 资源之前，将每个 StatefulSet 首次预检时的实际副本数写入当前 JobInfo 的私有版本化检查点。检查点同时保存 step/substep 执行键和是否已完成预检的状态；预检提前失败时，最终 JobInfo 只写入 `prepared=false` 的执行身份标记，后续同一执行可以重新预检并补齐副本快照。Workflow 进程重启并重新调度同一 `taskId` 时，只复用相同执行键且 `prepared=true` 的检查点，不会读取同任务中其他 reset step 的副本数，也不会把已经缩容为 0 的实时 StatefulSet 误当成原始状态。检查点不保存 SQL URL。

预检通过后，`database_reset` JobTask 对每个目标 `store` 组件执行：

1. 通过组件当前配置生成对应 StatefulSet 和 PVC 目标。
2. 获取集群中现有 StatefulSet；不存在时任务失败。
3. 将 StatefulSet 缩容到 0。
4. 等待该 StatefulSet selector 匹配的 Pod 全部消失。
5. 仅当该 StatefulSet 含有 initContainer `SQL_URL` 且请求 URL 与现值不同，使用冲突重试更新该环境变量；其他 PodTemplate 字段保持不变。值相同时跳过模板更新。
6. 删除目标 PVC 并等待 PVC 消失；`volumeClaimTemplates` PVC 只匹配 `{templateName}-{statefulSetName}-{ordinal}` 且 `ordinal` 为纯数字的名称。
7. 对 standalone PVC，使用删除前的 PVC Spec 重建，并清空 `spec.volumeName`。
8. 对 `volumeClaimTemplates` 生成的 PVC，不主动重建；缺失的 template PVC 不视为失败，由 StatefulSet 恢复副本时重新创建。
9. 将 StatefulSet 恢复到组件保存的 `replicas`；小于 1 时按 1。
10. 等待数据库组件 Ready。

数据库组件全部重置完成后，JobTask 直接结束，不查询、扩缩或 patch 同应用下的 `webservice` Deployment，也不修改这些组件的 `status`、`ready_replicas`、`last_abnormal` 或 `kubectl.kubernetes.io/restartedAt`。该规则覆盖普通组件及所有 share 策略。

`database-reset` 不要求应用预先停止，也不会自动停止服务。运行中的 `webservice` 保持运行，已停止的组件保持 `Stopped`。需要受控停机时，调用方应自行编排 `/stop`、`/database-reset`、`/start`；其中受保护的 share 组件仍不会被单应用生命周期命令操作。

## PVC And PV Boundary

数据库重置只删除 PVC，不直接删除 PV。

PV 生命周期交给 StorageClass reclaim policy：

- `Delete`：底层存储可能随 PVC 删除被回收。
- `Retain`：PV 和底层数据是否保留由集群管理员处理。

standalone PVC 重建时会清空 `spec.volumeName`，避免新 PVC 继续绑定旧 PV。`volumeClaimTemplates` PVC 由 StatefulSet controller 按模板重新创建。

## Failure Semantics

- 任何数据库组件重置失败都会使 JobTask 和 Workflow task 失败。
- 已成功删除或重建的资源不回滚。
- SQL URL 更新在 PVC 删除之前执行；更新失败时会尝试恢复该 StatefulSet 首次预检、缩容前的实际副本数，然后返回失败。原始副本数为 0 时保持缩容状态且不等待 Ready。一旦 PVC 删除开始，仍沿用部分成功、不回滚语义。
- 副本检查点持久化失败、内容损坏、版本不受支持、缺少执行键、同一执行键存在重复记录或缺少当前 StatefulSet 目标时，任务会在任何组件状态或 Kubernetes 资源变更前失败，不会静默使用当前副本数或其他 reset step 的检查点回退。旧版未携带执行键的检查点同样按不安全恢复数据拒绝。
- PVC 删除、StatefulSet 不存在、Pod 等待超时、PVC 删除等待超时、Ready 等待超时都会返回失败状态。
- timeout 会映射为 workflow job 的 timeout 状态。

## Curl Example

```bash
curl -X POST http://127.0.0.1:8000/api/v1/applications/app-123/database-reset \
  -H 'Content-Type: application/json' \
  --data @examples/database-reset-workflow/01-database-reset-request.json
```

响应中的 `taskId` 可继续通过 workflow task 查询接口观察异步执行结果。
