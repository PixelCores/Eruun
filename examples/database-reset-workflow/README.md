# Database Reset Workflow 示例

本目录提供数据库重置 API 的请求和响应示例。该接口只创建并入队 WorkflowQueue task，实际数据库 PVC 重置由 workflow worker 异步执行。

详细 API 文档见 [docs/database-reset-workflow.md](../../docs/database-reset-workflow.md)。

```bash
export ERUUN_API_URL=http://127.0.0.1:8000
export APP_ID=app-123
```

## 重置指定数据库组件

调用方必须显式传入当前应用下的 `store` 组件名；接口不会默认重置全应用。`initSqlUrl` 是可独立初始化空库的完整 SQL 快照，只会更新含 initContainer `SQL_URL` 的 StatefulSet，Redis 不使用该字段。

```bash
curl -sS -X POST "$ERUUN_API_URL/api/v1/applications/$APP_ID/database-reset" \
  -H "Content-Type: application/json" \
  --data @examples/database-reset-workflow/01-database-reset-request.json
```

请求体：

```json
{
  "components": ["mysql", "redis"],
  "initSqlUrl": "https://files.example/game-1.0.8.sql"
}
```

成功响应示例见 `02-database-reset-response.json`。响应中的 `data.taskId` 可用于继续查询异步执行状态：

```bash
TASK_ID=task-database-reset-001

curl -sS "$ERUUN_API_URL/api/v1/workflow/tasks/$TASK_ID/status"
```

## 错误示例

如果 `components` 为空、组件不存在，或组件不是 `componentType=store`，接口返回应用配置错误。示例见 `03-database-reset-invalid-component-response.json`。

## 文件

| 文件 | 用途 |
| --- | --- |
| `01-database-reset-request.json` | 使用当前版本初始化 SQL 重置 `mysql`，并同时按普通流程重置 `redis`。 |
| `02-database-reset-response.json` | 成功入队后的统一响应 envelope 示例。 |
| `03-database-reset-invalid-component-response.json` | 未知组件或非 store 组件的错误 envelope 示例。 |
