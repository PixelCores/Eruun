# Reset Workflow 示例

这些示例用于定义显式 reset workflow：先清理指定组件资源，再重新部署。全量清理/全量部署已迁移到 `/api/v1/applications/:appID/version` 的保留组件动作，见 `examples/version-update/12-cleanup-all-resources.json`、`13-deploy-all-components.json`、`14-recreate-all-components.json`。

## 文件

| 文件 | 用途 |
|------|---------|
| `01-reset-single-component-workflow.json` | 创建或更新一个针对 `web` 组件的 reset workflow。 |
| `02-reset-multi-component-workflow.json` | 创建或更新一个多组件 reset workflow。 |
| `03-exec-reset-workflow-request.json` | 通过 `workflowId` 执行 reset workflow。 |

## 创建或更新

```bash
APP_ID="your-app-id"

curl -sS -X PUT "http://127.0.0.1:8000/api/v1/applications/${APP_ID}/workflow" \
  -H "Content-Type: application/json" \
  -d @examples/reset-workflow/01-reset-single-component-workflow.json
```

响应中的 `workflowId` 用于后续执行。

## 执行

```bash
APP_ID="your-app-id"

curl -sS -X POST "http://127.0.0.1:8000/api/v1/applications/${APP_ID}/workflow/exec" \
  -H "Content-Type: application/json" \
  -d @examples/reset-workflow/03-exec-reset-workflow-request.json
```

执行前，把 `03-exec-reset-workflow-request.json` 里的 `wf-reset-example` 替换成创建或更新请求返回的 `workflowId`。

## 注意事项

- 清理步骤使用 `jobType: "cleanup_resources"`。
- 部署步骤使用 `jobType: "deploy"`，也可以省略 `jobType`。
- `workflowType` 是外层工作流分类，自定义 reset workflow 使用 `workflow`。
- `cleanup_resources` 会删除被引用组件归属的资源，并等待资源和 Pod 消失后再进入下一步。
- 全量 reset 使用 `/version` 的 `remove cleanup_all` / `add all`。
- 所有引用的组件名必须已存在于应用中。
