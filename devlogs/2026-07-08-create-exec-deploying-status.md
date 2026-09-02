# Create And Exec Deploying Status

## 背景与需求

`POST /api/v1/applications/create-and-exec` 创建应用并立即排队初次部署 workflow 后，组件运行态在 informer 或 job 结果回写前仍可能是空状态或 `Not Deploy`。此前 APP 聚合状态会短暂返回 `not_deploy`；PR 初稿曾把该窗口提升为 `updating`，但这会和 `/version` 更新语义混淆。需要新增独立的初次部署状态。

## 影响范围

- API: `GET /api/v1/applications/:appID/status` 与 `POST /api/v1/applications/components/status` 可返回 `deploying`。
- Domain: create-and-exec 立即排队成功后，将本次 workflow 中 `workflowType` 为空或 `deploy` 的 component step/substep 覆盖、空状态或 `Not Deploy` 且具备部署终态回写路径的组件标记为 `Deploying`。
- DB: 无 schema 变化；复用 `eruun_app_components.status`。
- Cache: 标记组件部署中后失效应用组件缓存。
- K8s / Job result: informer 或 job 结果同步保留 `Deploying`，直到组件进入 `Running` 或 `Failed`。
- Workflow: 不新增 workflow task 状态。

## 技术选型与取舍

- 新增 `Deploying` 组件态，而不是仅在聚合层临时覆盖，避免组件明细和应用聚合状态解释不一致。
- `deploying` 只表达初次部署中；`updating` 继续表达 `/version` 更新中。
- 只标记本次 workflow 部署类 step/substep 覆盖、空状态或 `Not Deploy` 且具备终态同步路径的组件，避免未被自定义 workflow 覆盖的组件被误标记，也避免覆盖 `Running`、`Stopped`、`Cleaning`、`Updating` 等已有语义以及 `cloudjob` 这类缺少组件运行态终态回写的组件卡在 `Deploying`。
- 延迟执行的 create-and-exec 不提前标记 `Deploying`。
- 普通 `POST /api/v1/applications/:appID/workflow/exec` 任务不作为 `deploying` 的判断依据，避免把非初次部署 workflow 误判为初次部署中。

## 实现摘要

- 新增 `ComponentStatusDeploying = "Deploying"`，聚合状态输出为 `deploying`。
- 聚合优先级调整为 `failed > deploying > updating > restarting > starting > cleaning > pending > running > stopped > not_deploy > unknown`。
- create-and-exec 成功获得即时 taskID 后调用应用服务，按选定 workflow 作用域标记初次部署组件；应用聚合状态只基于已持久化的 `Deploying` 组件态返回 `deploying`。
- read path 在 `Deploying` 组件已 Ready 时按 `Running` 返回；informer 对 `Pending` / `Unknown` 保留 `Deploying`，对 `Running` / `Failed` 允许覆盖。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/interfaces/api ./pkg/apiserver/domain/service/application ./pkg/apiserver
go test ./...
git diff --check
```

验收口径：即时 create-and-exec 初次部署状态返回 `deploying`，组件明细仅对本次 workflow 部署类 step/substep 覆盖的组件返回 `Deploying`；即时 `/version` 仍返回 `updating`；延迟执行、终态任务和非初次部署状态不提前显示 `deploying`。

## 风险与后续

`deploying` 是新的对外状态字符串，前端和上游调用方需要按新增枚举处理。若未来要区分 workflow 阶段，应新增状态详情字段或 task 查询，而不是继续扩展聚合状态字符串。
