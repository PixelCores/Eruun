# Resource Import 异步任务示例

本目录演示一次性扫描和纳管两个独立 Job。客户端不需要预估执行时间，也不要保持 HTTP 长连接。

1. 修改 `01-scan-request.json` 中的 namespace 和用户规则，提交扫描任务。
2. 使用返回的 `taskId` 轮询任务，等待 `status` 变为 `completed`。
3. 从扫描结果选择 root workload，填写 `02-manage-request.json` 并提交纳管任务。
4. 使用新的 `taskId` 查询纳管结果。扫描任务不会持续监听以后创建的资源。

```bash
curl -X POST \
  "$ERUUN_API_URL/api/v1/resource-import/jobs/scan" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -H "Content-Type: application/json" \
  --data @examples/resource-import-jobs/01-scan-request.json

curl \
  "$ERUUN_API_URL/api/v1/resource-import/jobs/<scan-task-id>" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID"

curl -X POST \
  "$ERUUN_API_URL/api/v1/resource-import/jobs/manage" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -H "Content-Type: application/json" \
  --data @examples/resource-import-jobs/02-manage-request.json
```

示例 task ID 是占位值。纳管前如果候选 workload 的 UID、resourceVersion 或 spec digest 已变化，任务会失败，调用方应重新扫描。
