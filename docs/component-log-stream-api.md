# 组件日志流 API（SSE）

> 状态：Current。当前路由为 `GET /api/v1/applications/:appID/components/:componentName/logs`。

## 接口说明
- 方法：GET
- 路径：`/api/v1/applications/:appID/components/:componentName/logs`
- Query 参数：
  - `container`（可选）：指定要拉取日志的容器名（仅普通容器）。
- 返回：`text/event-stream`（SSE）

## 行为说明
- 根据 `appID` 与 `componentName` 标签筛选 Pod。
- 默认选择最新的 Running/Ready Pod；若没有 Ready Pod，则选择最新的 Running Pod。
- 若没有 Running Pod，则选择最新的 Succeeded/Failed Pod 并一次性返回日志（连接随后结束）。
- 仅存在 Pending Pod 时返回 409，提示：`The component is pending scheduling.`
- 容器选择策略：
  - 若传入 `container`，按该容器名匹配；若容器不存在返回 400（`invalid container name for component logs`）。
  - 若未传入 `container`，优先匹配与组件名归一化一致的容器（主容器）；未命中时回退到 Pod 的第一个普通容器。
- 返回响应头：
  - `X-Eruun-Pod`
  - `X-Eruun-Container`
- 即使客户端声明 `Accept-Encoding: gzip`，SSE 响应也不启用 gzip，确保每次 Flush 都能立即送达事件。
- API Server 会清除此 SSE 响应的全局写 deadline；日志流不会被默认 30 秒 `WriteTimeout` 截断。反向代理、客户端和上游日志流仍可各自限制连接生命周期。

## 使用示例
```bash
curl -N http://127.0.0.1:8000/api/v1/applications/app-123/components/api/logs
```

指定容器：
```bash
curl -N "http://127.0.0.1:8000/api/v1/applications/app-123/components/api/logs?container=sidecar"
```

SSE 数据格式示例：
```
data: 2026-01-20T10:00:00Z app started

data: 2026-01-20T10:00:01Z ready
```
