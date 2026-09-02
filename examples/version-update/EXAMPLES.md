# 版本更新 API 示例文件说明

## 示例文件列表

| 文件 | 说明 |
|------|------|
| `01-simple-image-update.json` | 简单镜像更新 |
| `02-scale-replicas.json` | 扩容副本数 |
| `03-add-component.json` | 新增组件 |
| `04-remove-component.json` | 删除组件 |
| `05-mixed-operations.json` | 混合操作 |
| `06-canary-release.json` | 金丝雀发布 |
| `07-update-env.json` | 更新环境变量 |
| `08-version-bump-only.json` | 仅更新版本号 |
| `09-update-with-workflow.json` | 指定自动执行工作流（含 `executeAt` 延迟执行示例） |
| `10-cancel-delayed-update.json` | 取消待执行延迟更新任务 |
| `11-update-with-task-callback.json` | 指定本次版本更新 workflow task 的独立终态回调 |
| `12-cleanup-all-resources.json` | 清理当前应用全部 DB 已知组件资源，保留组件记录 |
| `13-deploy-all-components.json` | 通过选定 workflow 部署当前应用全部 DB 已知组件 |
| `14-recreate-all-components.json` | 先清理全部组件资源，再部署全部组件 |
| `15-restart-component.json` | 不修改组件规格，重启已有 webservice/store 组件 |

## 使用方法

```bash
# 替换 APP_ID 为实际的应用 ID
APP_ID="your-app-id"

# 使用示例文件发送请求
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @01-simple-image-update.json

# 使用 task 级 callback 示例
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @11-update-with-task-callback.json

# 使用 /version 保留动作进行全量重建
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @14-recreate-all-components.json

# 重启已有组件
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @15-restart-component.json

```

## 注意事项

- `version` 字段是必填项
- 组件名称会自动转换为小写
- `autoExec` 默认为 `true`，会自动触发工作流执行
- 可通过 `workflowId` 指定自动执行的工作流
- `executeAt` 为 Unix 秒时间戳，仅在 `autoExec=true` 且有组件变更或资源动作并创建 workflow task 时生效
- `callback` 只在 `autoExec=true` 且本次请求创建 workflow task 时生效；不会覆盖 App 或 Workflow 默认 callback
- 全量清理/全量部署使用 `components[]` 保留动作：`remove cleanup_all` 与 `add all`
- 组件重启使用 `{"action":"restart","name":"<component>"}`，仅支持已有 `webservice` / `store` 组件，且要求 `autoExec=true`

