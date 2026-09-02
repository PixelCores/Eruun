# 2026-06-22 Log Archive Download And JobType

## Context

需要让用户获取服务日志类文件。最初方案偏向 workflow 异步上传：用户按组件发起日志归档任务，系统读取路径、打包 zip、上传静态文件服务，再通过任务结果获取归档 URL。

后续需求收敛为第一版先直接返回文件流：调用方明确给出组件和容器内日志路径，API 从目标 Pod 读取并同步返回归档内容，不再要求用户理解 workflow/task/uploader。

## Decisions

- `POST /api/v1/applications/:appID/log-archives` 改为同步文件流接口，不再创建 workflow/task，不再返回 `workflowId/taskId`。
- 第一版只支持单组件、单路径：`components` 必须恰好 1 个，`path` 必填。
- 接口接受 `name/jobType/mode` 以兼容当前表单形态，但只有 `components/path/container` 参与执行；`jobType` 传入时必须是 `log_archive_upload`。
- 同步接口复用组件文件导出链路，继承 Pod 选择、容器校验、路径归档和 zip/multipart stream 行为。
- 拒绝非 Pod 型组件，避免把配置类组件误报成 Pod 不可用。
- 保留专用 workflow jobType：`log_archive_upload`，用于高级异步 workflow 场景；该路径仍依赖 `properties.path` 和 `ArchiveUploader`。
- 暂不公开通用 `shell_exec` jobType；Shell/Archive 能力只作为内部 helper 复用。

## Notes

后续如果需要多组件或多路径下载，应单独设计合包命名和错误语义，不在第一版同步接口里隐式扩展。
