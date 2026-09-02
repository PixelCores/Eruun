# 组件 Pod 文件导出与 Shell 执行 API

> 状态：Current。当前路由覆盖文件导出、非流式 Shell 执行与 SSE Shell 执行。

## 概览

新增三个应用组件维度的 API：

- `POST /api/v1/applications/:appID/components/:componentName/files/export`
- `POST /api/v1/applications/:appID/components/:componentName/shell/exec`
- `POST /api/v1/applications/:appID/components/:componentName/shell/stream`

接口按 `appID + componentName` 定位组件，并自动选择最新的就绪 Running Pod；如果没有 Ready Pod，则退化到最新的 Running Pod。

## 目标 Pod 选择规则

- 优先选择最新的 Ready Running Pod
- 若没有 Ready Pod，则选择最新的 Running Pod
- 仅有 Pending Pod 时返回 `409`
- 没有可用 Pod 时返回 `404`

## 导出组件文件

### 请求

`POST /api/v1/applications/:appID/components/:componentName/files/export`

```json
{
  "path": "/tmp/output",
  "container": "api"
}
```

### 行为

- `path` 必填，只支持单一路径
- 该路径可以是文件或目录
- 若容器中存在 `tar`，响应体返回 `application/zip` 文件流
- 若容器中缺少 `tar`，自动降级返回 `multipart/mixed` 文件流（递归普通文件，忽略目录与符号链接）
- 响应头包含：
  - `Content-Type`（`application/zip` 或 `multipart/mixed`）
  - `Content-Disposition`
  - `X-Eruun-Pod`
  - `X-Eruun-Container`

### curl 示例

```bash
curl -X POST \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/api/files/export" \
  -H 'Content-Type: application/json' \
  -o api-export.zip \
  -d '{
    "path": "/tmp/output",
    "container": "api"
  }'
```

## 执行组件 Shell 脚本

### 请求

`POST /api/v1/applications/:appID/components/:componentName/shell/exec`

```json
{
  "script": "echo hello && ls /tmp",
  "container": "api"
}
```

### 返回

```json
{
  "code": 0,
  "message": "",
  "data": {
    "namespace": "default",
    "podName": "api-7c9bbd7d4f-9qzqx",
    "containerName": "api",
    "stdout": "hello\n",
    "stderr": "",
    "exitCode": 0,
    "succeeded": true
  }
}
```

### 行为

- `script` 必填，默认使用 `/bin/sh -c` 执行
- `container` 可选；不传时优先匹配组件同名容器，否则取第一个具名容器
- 命令非零退出码不会转成 HTTP 5xx，而是通过 `exitCode` 和 `succeeded` 返回
- `stdout` / `stderr` 返回大小各自上限 `1 MiB`，超限会截断并追加 `...[truncated]` 标记

### curl 示例

```bash
curl -X POST \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/api/shell/exec" \
  -H 'Content-Type: application/json' \
  -d '{
    "script": "echo hello && ls /tmp",
    "container": "api"
  }'
```

## 流式执行组件 Shell 脚本

### 请求

`POST /api/v1/applications/:appID/components/:componentName/shell/stream`

```json
{
  "script": "echo hello && ls /tmp",
  "container": "api"
}
```

### 行为

- 使用 SSE（`text/event-stream`）返回流式结果
- 事件类型：
  - `stdout`：标准输出增量片段
  - `stderr`：标准错误增量片段
  - `exit`：结束事件，包含 `exitCode` 与 `succeeded`
  - `error`：流式执行异常（非命令退出码类错误）
- 响应头包含：
  - `X-Eruun-Pod`
  - `X-Eruun-Container`
- 即使客户端声明 `Accept-Encoding: gzip`，SSE 响应也不启用 gzip，确保输出事件不会因压缩缓冲延迟。
- API Server 会清除此 SSE 响应的全局写 deadline；流式 Shell 不会被默认 30 秒 `WriteTimeout` 截断。反向代理、客户端和命令执行本身仍可各自限制连接生命周期。

### curl 示例

```bash
curl -N -X POST \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/api/shell/stream" \
  -H 'Content-Type: application/json' \
  -d '{
    "script": "echo hello && ls /tmp",
    "container": "api"
  }'
```

## 限制

- Shell 执行（同步/流式）依赖目标容器内存在 `/bin/sh`
- 文件导出优先使用容器内 `tar`；若缺失会尝试降级到 `find + cat` 组合
