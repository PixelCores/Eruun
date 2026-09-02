# Version Update Execution Scope

## 背景与需求

PaaS 在 `/version` 中只提交单个组件更新时，仍可能复用包含 mysql、redis、backend、frontend、socket 的完整 workflow。当前默认语义会执行所选 workflow 的完整步骤，即使请求只更新 frontend，也会重新生成 mysql/redis 相关 job。调用方希望不创建新 workflow，也能表达“本次只执行实际变更组件”。

## 影响范围

- API: `/version` 与 `/version/diff-update` 新增 `executionScope`，响应返回归一化后的执行范围。
- Domain: `/version` auto-exec task 在事务内根据真实 `updatedComponents + addedComponents` 写入 task-scoped execution components。
- DB: 无 schema 变化；复用 `workflow_queue.resource_action_info`。
- Cache: 无变化。
- K8s: 不改变资源调和器；只改变本次 workflow task 生成哪些 deploy/默认 component jobs。
- Workflow: worker 在生成 jobs 前按 task-scoped execution components 过滤 workflow policies。

## 技术选型与取舍

- 选择新增 `executionScope`，而不是把 `changed_components` 放进 `strategy`。`strategy` 表达发布策略，`executionScope` 表达 workflow 执行范围，两者拆开能避免 PaaS 和 Eruun 对同一字段产生歧义。
- 选择复用 `resource_action_info`，而不是新增表或 workflow queue 字段。该 JSON 已用于版本更新 task 的临时资源动作和 Ready 目标，适合承载 task-scoped 执行范围。
- `changed_components` 只记录真实 `updatedComponents + addedComponents`，不自动推断依赖组件。依赖 config/secret 需要调用方明确提交，或使用默认 `full_workflow`。
- `changed_components` 与 `add all` / `remove cleanup_all` fail-fast，因为这两个动作语义上要求全量资源处理。
- workflow 定义不落库修改；过滤只作用于本次 task 的内存 steps，避免一次版本更新污染后续 workflow 执行。

## 实现摘要

- `UpdateVersionRequest`、`DiffUpdateVersionRequest` 和 `UpdateVersionResponse` 增加 `executionScope`。
- 新增 `full_workflow` 与 `changed_components` 执行范围常量，空值默认 `full_workflow`，非法值返回应用配置错误。
- auto-exec 事务内计算真实变更组件，写入 `VersionUpdateResourceActionInfo.executionScope` 与 `executionComponents`。
- worker 解析 `resource_action_info` 后，在 `GenerateJobTasks` 生成 deploy/默认 component jobs 前过滤 workflow steps；approval steps 保留，未命中的 component steps 不生成 jobs。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/domain/service/application ./pkg/apiserver/event/workflow ./pkg/apiserver/interfaces/api
go test ./...
```

验收口径：默认请求仍执行完整 workflow；`executionScope=changed_components` 只执行真实变更组件命中的 deploy/默认 workflow jobs；非法 scope 或与全量资源动作混用会失败；diff-update 会透传该字段。

## 风险与后续

`changed_components` 不会自动补齐依赖资源。调用方如果需要同步 config/secret，必须把对应组件作为本次变更提交，或继续使用默认 `full_workflow`。`database_reset`、`log_archive_upload` 等非部署 jobType 不属于 `changed_components` 自动执行范围，需要使用对应专用 API/workflow 或默认 `full_workflow`。
