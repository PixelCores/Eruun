# API Payload Field Aliases

## 背景与需求

部分公开请求字段和响应字段命名不一致：创建应用使用 `component`，而其他接口和响应普遍使用 `components`；更新 workflow 使用 `workflow` 承载步骤，而读取 workflow 返回 `steps`；读侧 workflow step 返回 `workflowType`，但新请求更适合使用 `jobType` 表达单步任务类型。

本次目标是在不破坏旧调用方的前提下补齐更自然的字段别名，并保证读接口返回的 payload 可以安全编辑后提交到更新接口。

## 影响范围

- API: 创建应用、创建并执行、Try Application、Try Workflow、更新 workflow 的 JSON 绑定。
- Domain: 无业务规则变化。
- DB: 无 schema 或持久化结构变化。
- Cache: 无变化。
- K8s: 无变化。
- Workflow: 仅请求 DTO 字段别名，workflow 内容语义不变。

## 技术选型与取舍

- 选择在 DTO strict JSON 解析层实现别名，避免把兼容字段扩散到 domain model、repository 或 workflow 持久化结构。
- `components` 和 `component`、`steps` 和 `workflow`、`jobType` 和 `workflowType` 同时出现时直接报错，避免两个字段值不一致时出现隐式优先级。
- 保持响应结构最小化，不额外复制响应字段；读侧已有 `steps` / `workflowType` / `properties[]`，请求侧接受这些字段即可满足 read-update round trip。

## 实现摘要

- `CreateApplicationsRequest` 接受 `components` 作为 `component` 的兼容别名。
- `UpdateApplicationWorkflowRequest` 接受 `steps` 作为 `workflow` 的兼容别名。
- `POST /applications/:appID/workflow/try` 复用更新 workflow 请求形状，同样接受 `steps` 作为 `workflow` 的兼容别名。
- `CreateWorkflowStepRequest` 和 `CreateWorkflowSubStepRequest` 接受 `workflowType` 作为 `jobType` 的兼容别名。
- `CreateWorkflowStepRequest` 和 `CreateWorkflowSubStepRequest` 接受读响应中的 `properties[]` 数组，并按数组元素保留多条 workflow policies。
- `properties[]` 数组中的 `policies` 会在持久化前与旧字段路径一样执行组件引用归一化，避免 read-edit-submit 后执行阶段因大小写不一致跳过组件。
- 空 `properties[]` 按未提供 properties 处理，仍会保留并校验显式传入的 `components`。
- Try Workflow 校验会遍历 `properties[]` 的每个元素，避免仅校验首个元素后放过后续缺失 path 或引用不存在组件的条目。
- Try Workflow 会校验顶层 `workflowType`，并与实际更新 workflow 的任务类型白名单保持一致。
- 多项 `properties[]` 不允许跨条目重复引用同一组件，避免持久化后非日志归档 job 对同一组件重复调度。
- 多项 `properties[]` 如果同时提供 `components`，`components` 必须与 `properties[].policies` 的并集一致，避免显式组件被静默忽略。
- 文档和新示例优先展示 `components`、`steps` 和 `jobType`。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/interfaces/api/dto/v1 ./pkg/apiserver/interfaces/api ./pkg/apiserver/domain/service/application
git diff --check
```

## 风险与后续

旧字段仍可用，因此主要风险是调用方同时发送新旧字段。本次以明确错误响应处理冲突，后续如果需要移除旧字段，应另行制定版本迁移窗口。
