# 组件日志流示例

该目录用于记录组件日志流接口的调用示例。

## 示例
```bash
curl -N http://127.0.0.1:8000/api/v1/applications/app-123/components/api/logs
```

指定容器（例如 sidecar）：
```bash
curl -N "http://127.0.0.1:8000/api/v1/applications/app-123/components/api/logs?container=sidecar"
```
