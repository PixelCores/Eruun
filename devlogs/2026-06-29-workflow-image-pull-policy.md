# Workflow ImagePullPolicy Default

## 背景与需求

Workflow 生成的业务容器原先没有统一到总是拉取镜像。当前需求是让 workflow 部署和仓库安装清单默认总是拉取镜像，避免复用节点本地旧镜像。

## 影响范围

- API: 无请求或响应字段变化。
- Domain: 无 DB 模型或应用实体变化。
- DB: 无 schema 变化。
- Cache: 无变化。
- K8s: Workflow 生成的 Deployment、StatefulSet、Job、CronJob、init container 和 sidecar container 默认 `imagePullPolicy` 改为 `Always`。
- Workflow: 资源生成默认值变化；已有非 `Always` Deployment 会在下一次 workflow 调和时被判定为需要更新。

## 技术选型与取舍

- 使用 `config.DefaultWorkflowImagePullPolicy` 作为唯一默认值来源，避免在 job 和 trait 包重复硬编码。
- 不新增用户可配置字段。本次需求是默认值收敛，不扩大 API 或 DTO 契约。
- 同步更新安装清单和文档示例，保持仓库内显式默认值一致。

## 实现摘要

- Deployment、StatefulSet、Instant/Cron Job、init trait 和 sidecar trait 的容器生成统一使用 `config.DefaultWorkflowImagePullPolicy`。
- Deployment 差异检测新增 `ImagePullPolicy` 比较，确保旧策略和新默认策略不被误判为 no-op。
- 当前实现参考文档中的 init container 生成结果同步为 `Always`。

## 测试与验收

计划执行：

```bash
go test ./pkg/apiserver/event/workflow/job ./pkg/apiserver/workflow/traits
go test ./pkg/apiserver/config
git diff --check
```

## 风险与后续

`Always` 会增加镜像仓库访问频率，并可能暴露镜像仓库认证、限流或网络问题。没有引入 per-component override；后续如需精细化控制，应先设计明确的请求字段和兼容策略。
