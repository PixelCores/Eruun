# Version Update App Status

## 背景与需求

即时 `/api/v1/applications/:appID/version` 创建 workflow task 后，旧 Pod 可能仍然处于 Ready。应用状态接口此前只从组件运行态聚合，`Updating` 组件在 `ready_replicas >= replicas` 时会被读路径纠正为 `Running`，导致 workflow 仍在等待、排队或执行时，APP 聚合状态误报为 `running`。

## 影响范围

- API: `GET /api/v1/applications/:appID/status` 与 `POST /api/v1/applications/components/status` 的响应结构不变，聚合口径新增即时 `/version` 活跃 task 覆盖规则。
- Domain: `/version` auto-exec 创建真实 workflow task 时持久化最小 `resource_action_info` marker，用于识别 version-update task。
- DB: 无 schema 变化；复用 `eruun_workflow_queue.resource_action_info`。
- Cache: 不改变组件明细缓存契约。
- Workflow: marker-only `resource_action_info` 不改变 full workflow job 生成；restart、workload Ready、changed_components 继续使用同一 payload 扩展字段。

## 技术选型与取舍

- 不新增 `app.status` 字段，避免在组件运行态和 workflow task 状态之外引入第三个事实源。
- 不改变组件明细接口；组件明细仍面向排障展示底层组件运行态。
- 只让即时 `/version` 的活跃 workflow task 覆盖 APP 聚合状态。未来 `executeAt` 的延迟任务不提前显示 `updating`，终态任务不继续覆盖状态。
- 只覆盖 `running`、`pending`、`stopped`、`not_deploy`、`unknown` 这类可用性状态；`failed`、`restarting`、`starting`、`cleaning` 等更具体状态保持优先。

## 实现摘要

- `/version` auto-exec 真实 workflow task 即使没有 restart、workload Ready 或 changed_components 信息，也会写入 `source=version_update_action, version=1` 的 marker。
- APP status 聚合后按 app 定向查询活跃 workflow tasks；若存在即时活跃 version-update marker task，则把可覆盖状态提升为 `updating`。
- marker-only `resource_action_info` 进入 workflow job builder 时仍按默认 full workflow 执行，不裁剪步骤、不追加 restart job。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/domain/service/application ./pkg/apiserver/interfaces/api ./pkg/apiserver/event/workflow
go test ./... -race -cover
git diff --check
```

验收口径：即时 `/version` workflow task 活跃时，单应用和批量 APP 状态返回 `updating`；延迟任务、终态任务、非 `/version` workflow 不覆盖；更高优先级状态不被降级；marker-only task 不改变 full workflow job 生成。

## 风险与后续

状态接口会多读取一次该 app 的 workflow task 列表，仅在组件聚合结果属于可覆盖状态时发生。若未来需要展示具体更新任务信息，应新增显式任务查询或状态详情字段，而不是继续扩展聚合状态字符串。
