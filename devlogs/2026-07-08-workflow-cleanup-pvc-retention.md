# Workflow Cleanup PVC Retention

## 背景与需求

workflow cleanup 之前会删除 PVC：`cleanup_resources` 会按组件生成的 `AdditionalObjects` 删除 standalone PVC，也会按组件标签删除 PVC；失败 job 的局部 cleanup 也可能删除 PVC deploy job 创建的 PVC。对 PaaS 试玩日志这类通过 `claimName` 复用的 PVC，这会把应长期保留的数据卷纳入普通部署失败清理。

## 影响范围

- Workflow: `cleanup_all`、`cleanup_failed` 和 `cleanup_resources` 不主动删除 standalone PVC。
- Storage Trait: `tmpCreate=false` 的 standalone PVC 仍作为 deploy/database-reset target 可见，但 cleanup 保留。
- StatefulSet: workflow cleanup 按普通 StatefulSet delete 删除；`volumeClaimTemplates` PVC 生命周期由 Kubernetes retention policy 决定。
- Database Reset: 保持现有 PVC 删除/重建语义；它仍是重置数据库 PVC 数据的专用 workflow。
- API/DB: 无路由、DTO、响应或 schema 变化。

## 技术选型与取舍

- 不新增配置开关。standalone PVC 保留作为 cleanup 的固定数据安全边界，避免调用方在失败清理和数据重置之间误选。
- 不移除 storage trait 生成的 PVC target。`database-reset` 和 PVC deploy 仍依赖该 target 来定位 standalone PVC。
- 同步 cleanup 和 workflow cleanup 共用 standalone PVC 不主动删除的边界；API 形态不变。

## 实现摘要

- `DeployPVCJobCtl.Clean` 改为保留 PVC，不再删除 cleanup tracker 中记录的 PVC。
- `CleanupResourcesJobCtl` 跳过 `AdditionalObjects` 中的 standalone PVC，并停止按标签列出/删除 PVC。
- StatefulSet workflow cleanup 使用普通 delete，不强制修改 `volumeClaimTemplates` PVC retention policy。
- 文档同步说明 cleanup 保留 standalone PVC，`database-reset` 保持删除/重建 PVC 能力。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/event/workflow/...
go test ./pkg/apiserver/domain/service/application/...
go test ./... -race -cover
go vet ./...
git diff --check
```

验收口径：显式 `claimName` PVC、嵌套 standalone storage PVC、标签命中的 PVC、PVC deploy job 记录的 PVC 在 cleanup 后保留；StatefulSet `volumeClaimTemplates` PVC 由 Kubernetes retention policy 决定；非 PVC 资源仍按原 cleanup 规则删除。
