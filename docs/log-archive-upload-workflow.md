# Log Archive Download

> 状态：Current。本文描述组件日志归档的同步下载 API，以及高级 workflow jobType 的保留边界。

## Endpoint

```http
POST /api/v1/applications/:appID/log-archives
Content-Type: application/json
```

请求体：

```json
{
  "name": "archive-worker",
  "jobType": "log_archive_upload",
  "mode": "StepByStep",
  "components": ["worker"],
  "path": "/data/logs/archive",
  "container": "worker"
}
```

- `components` 必填，第一版必须恰好 1 个组件名。
- `path` 必填，表示目标容器内要归档的日志文件或目录路径。
- `container` 可选；不传时优先匹配组件同名容器，否则选择第一个具名容器。
- `jobType` 可选；传入时必须是 `log_archive_upload`。
- `name` / `mode` 可选；同步下载接口接受但不参与执行。
- 组件必须存在且会产生 Pod：`webservice`、`store`、`job`、`scheduledjob`。

成功响应不使用统一 JSON envelope，而是直接返回文件流：

- `Content-Type`: 通常为 `application/zip`
- `Content-Disposition`: 下载文件名
- `Cache-Control: no-store`
- `X-Eruun-Pod`: 实际读取的 Pod
- `X-Eruun-Container`: 实际读取的容器

错误响应仍使用统一错误 envelope。空组件、多组件、未知组件、非 Pod 型组件、错误 `jobType` 返回 `ErrApplicationConfig`；空路径或无效路径返回 `ErrComponentFilePathInvalid`。

## Execution Behavior

同步下载接口不会创建 workflow、不会写入 `eruun_workflow` / `eruun_workflow_queue`，也不会上传到静态文件服务。

执行流程：

1. 校验应用、组件、单组件单路径请求形态。
2. 拒绝不会产生 Pod 的组件类型。
3. 按 `appID + componentName` label 定位组件 Pod。
4. 复用组件文件导出的 Pod 选择规则：优先最新 Ready Running Pod，其次最新 Running Pod。
5. 校验请求容器存在；未指定容器时按组件同名容器优先。
6. 读取 `path` 指向的文件或目录并直接写入 HTTP response。

归档行为沿用组件文件导出能力：

- 目标容器存在 `tar` 时返回 `application/zip`。
- 目标容器缺少 `tar` 时保留现有 fallback，返回 `multipart/mixed`。
- 响应体是流式传输；服务端会在写 header 前探测首字节，以便空流或路径错误仍能返回 JSON 错误。

## Curl Example

```bash
curl -X POST http://127.0.0.1:8000/api/v1/applications/app-123/log-archives \
  -H 'Content-Type: application/json' \
  -o worker-logs.zip \
  -d '{
    "name": "archive-worker",
    "jobType": "log_archive_upload",
    "mode": "StepByStep",
    "components": ["worker"],
    "path": "/data/logs/archive",
    "container": "worker"
  }'
```

## Workflow JobType

公开 workflow jobType 仍保留：

```text
log_archive_upload
```

直接创建/更新 workflow 或调用 Try/DryRun 校验时，`log_archive_upload` step/subStep 仍要求提供 `properties.path`，且目标组件必须是会产生 Pod 的组件类型。

直接创建/更新 workflow 的请求示例：

```json
{
  "name": "log-archive-upload",
  "workflowType": "log_archive_upload",
  "steps": [
    {
      "name": "archive-api",
      "jobType": "log_archive_upload",
      "mode": "StepByStep",
      "components": ["api"],
      "properties": {
        "path": "/var/log/api",
        "container": "api"
      }
    }
  ]
}
```

历史 `workflow` 步骤字段仍兼容接收；新接入建议使用 `steps`。读接口返回的 `workflowType` step 字段也可作为 `jobType` 的兼容输入，`steps[].properties[]` / `subSteps[].properties[]` 数组可直接作为更新请求输入，用于支持读后编辑再提交。

兼容简写：当 `jobType=log_archive_upload` 且省略 `components` / `properties.policies` 时，step 或 subStep 的 `name` 会被视为组件名；这种写法仍必须提供 `properties.path`。

## Workflow Read Response

通过 `GET /api/v1/applications/:appID/workflows` 读取 workflow 时，`steps[].properties[]` 和 `subSteps[].properties[]` 会返回持久化的归档配置：

- `policies` 保留该 properties 项关联的组件名。
- `path` 保留该组件本次归档路径。
- `container` 保留可选容器名。

响应同时保留兼容字段 `steps[].components` / `subSteps[].components`，其值仍是从 `properties[].policies` 扁平化得到的组件名列表。

## Uploader Boundary

同步下载接口不依赖 uploader。只有 workflow jobType 执行异步上传时才需要注入 `ArchiveUploader`。

生产环境未配置 uploader 时，workflow JobTask 会 fail-fast：

```text
archive uploader is not configured
```

本能力不公开通用 `shell_exec` jobType。Shell/Archive 只作为内部 helper 供专用能力复用。
