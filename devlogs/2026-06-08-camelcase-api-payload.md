# CamelCase API Payload

## 背景与需求


本次需求明确为严格收敛对外 payload 命名，不继续兼容旧字段。

## 影响范围

- API: `ApplicationBase.workflow_id` 改为 `workflowId`；创建应用请求 `ID` 改为 `id`。
- Domain: 领域模型和 DB 字段不变。
- DB: 无变化。
- Cache: 列表缓存 key 升级为 `app:list:v2` / `app:template:list:v2`，旧 key 被自然弃用，避免旧缓存中的 `workflow_id` 反序列化后丢失默认 workflow。
- K8s: 无变化。
- Workflow: 内部 workflow/task/job 状态字段不变。

## 技术选型与取舍



## 实现摘要

- API DTO 将 `ApplicationBase.WorkflowID` 输出字段改为 `workflowId`。
- 创建应用请求将 `CreateApplicationsRequest.ID` 输入字段改为 `id`。
- strict JSON 解析不再对字段名做大小写兼容匹配，因此 `ID` 和 `Id` 会被拒绝。
- 当前对外文档和 examples 同步改为 camelCase。

## 测试与验收

计划执行：

- `go test ./pkg/apiserver/interfaces/api/dto/v1`
- `go test ./pkg/apiserver/interfaces/api`

验收口径：

- OAuth 协议字段、Bcode 内部字段、Domain model / DB 字段和 Historical/Audit 文档中的旧字段不作为本次目标处理。

## 风险与后续

- 这是破坏性 API 变更；仍使用旧字段的外部调用方需要迁移到 `workflowId` 和 `id`。
- 如果未来需要平滑迁移，应新增显式兼容窗口和弃用文档，而不是隐式恢复大小写宽松匹配。
