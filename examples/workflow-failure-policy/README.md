# Workflow Failure Policy Examples

本目录提供 workflow 级 `failurePolicy` 和 Job 级失败清理例外请求示例。`cleanup_all` 是 workflow 默认策略；`type=job` 可以通过 `properties.failurePolicy: cleanup_failed` 只为主 Kubernetes Job 退出全量清理。完整语义见 `docs/workflow-failure-policy.md`。

| 文件 | 路由 | 用途 |
| --- | --- | --- |
| `create-app-cleanup-all-request.json` | `POST /api/v1/applications` | 创建应用时使用 workflow 对象写法设置 `failurePolicy: cleanup_all` |
| `update-workflow-cleanup-all-request.json` | `PUT /api/v1/applications/:appID/workflow` | 更新已有 workflow 时在请求顶层设置 `failurePolicy: cleanup_all` |
| `version-add-job-cleanup-failed-request.json` | `POST /api/v1/applications/:appID/version` | 新增一次性 SQL Job，并通过 Job 级 `failurePolicy` 仅清理失败 Job |

Try/DryRun 示例见 `examples/validation-try/10-try-workflow-request.json`。
