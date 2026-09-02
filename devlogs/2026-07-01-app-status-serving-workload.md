# App Status Serving Workload

## 背景与需求

`POST /api/v1/applications/:appID/stop` 当前停止 `webservice` 对应的 Deployment，并保留 `store` 组件 StatefulSet 运行。批量应用状态接口此前按所有组件状态聚合，导致应用的 Deployment 已停止后，只要 `store` 组件仍是 `Running`，应用聚合状态仍返回 `running`。同一口径也需要覆盖 `/start` 和 `/restart`：`start` 恢复服务组件时应展示服务恢复状态，`restart` 触发任意工作负载重启时仍要展示全局重启状态。

## 影响范围

- API: `GET /api/v1/applications/:appID/status` 和 `POST /api/v1/applications/components/status` 的响应结构不变，聚合口径收敛为服务工作负载可用性。
- Domain: stop/start/restart 的资源操作不变，不新增生命周期实体。
- DB: 仍读取 `app_components.status`、`ready_replicas`、`last_abnormal`，无 schema 变化。
- Docs: 更新应用状态与批量状态 API 文档，明确 `webservice` 与 `store` 的聚合边界。

## 技术取舍

- 不扩大 stop/start 到 StatefulSet。现有 stop 合同只停止 Deployment，贸然停止 `store` 会改变数据类组件运行边界。
- 不新增单独状态字段。应用状态继续从组件状态聚合，避免引入新的事实源。
- 保留全组件异常优先级。`failed`、`updating`、`restarting`、`starting`、`cleaning` 仍由任意组件触发，避免隐藏非服务组件的异常或变更过程。
- 明确资源操作范围与状态语义分离。`start/stop` 仍只控制服务类 Deployment，应用聚合状态表达服务可用性；`restart` 仍可作用于 `webservice/store`，其 `Restarting` 状态保持全局可见。

## 实现摘要

- 聚合状态先扫描全组件状态，再单独扫描 `webservice` 组件状态。
- 全组件高优先级状态保持优先返回。
- 若存在 `webservice` 组件，`pending/running/stopped/not_deploy/unknown` 这类可用性结果以 `webservice` 组件为准；没有 `webservice` 时保持原有全组件聚合。
- 测试覆盖 stop 后 `webservice=Stopped + store=Running` 返回 `stopped`，start 后 `webservice=Starting + store=Running/Stopped` 返回 `starting`，restart 中 `store=Restarting` 返回 `restarting`。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/interfaces/api ./pkg/apiserver/domain/service/application
go test ./...
git diff --check
```
