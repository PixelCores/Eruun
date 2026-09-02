# 版本更新 API 示例

本目录包含版本更新 API 的各种使用场景示例。

详细 API 文档请参考: [docs/version-update-api.md](../../docs/version-update-api.md)

## 快速开始

```bash
# 替换 {appID} 为实际的应用 ID
APP_ID="your-app-id"

# 简单镜像更新
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @01-simple-image-update.json

# 本次版本更新 task 级 callback
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @11-update-with-task-callback.json

# 清理后全量部署
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @14-recreate-all-components.json

# 重启单个组件
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d @15-restart-component.json

# 取消待执行的延迟版本更新任务
curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version/cancel" \
  -H "Content-Type: application/json" \
  -d @10-cancel-delayed-update.json
```

## 示例文件说明

详见 [EXAMPLES.md](./EXAMPLES.md)
