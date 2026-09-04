# Namespace 同步纳管兼容接口脱敏示例

本目录展示保留给旧客户端的同步 `managementMode: "adopted"` 流程。新交互应使用 `examples/resource-import-jobs/` 的一次性扫描与异步纳管任务。资源名、UID、摘要和指纹均为占位值，不能直接用于生产 apply。

1. 提交 `01-dry-run-request.json`，检查所有资源的 source、ownership 与 disposition。
2. 保存响应中的 `data.planFingerprint`。
3. 保持映射不变，提交 `02-apply-request.json` 并替换为真实 dry-run 指纹。
4. 如需显式删除 adopted 非数据资源，先调用 cleanup-plan，再用 `03-cleanup-apply-request.json` 提交 cleanup 指纹。

```bash
curl -X POST \
  "$ERUUN_API_URL/api/v1/applications/import/namespace" \
  -H "Content-Type: application/json" \
  --data @examples/namespace-import/01-dry-run-request.json

curl -X POST \
  "$ERUUN_API_URL/api/v1/applications/<app-id>/resources/cleanup-plan"

curl -X DELETE \
  "$ERUUN_API_URL/api/v1/applications/<app-id>/resources" \
  -H "Content-Type: application/json" \
  --data @examples/namespace-import/03-cleanup-apply-request.json
```

只传 `namespace` 仍是 observe 兼容请求，不会自动接管。生产 apply 前必须重新 dry-run。
