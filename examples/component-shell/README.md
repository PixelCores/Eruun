# 组件 Shell 执行示例

该目录包含组件 Shell 同步执行与流式执行的请求示例。

## 同步执行（JSON 返回）

```bash
curl -X POST \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/api/shell/exec" \
  -H 'Content-Type: application/json' \
  -d @examples/component-shell/exec-component-shell-request.json
```

## 流式执行（SSE 返回）

```bash
curl -N -X POST \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/api/shell/stream" \
  -H 'Content-Type: application/json' \
  -d @examples/component-shell/stream-component-shell-request.json
```
