# Devlogs

`devlogs/` 用于记录重要代码变更背后的需求背景、技术选型和取舍理由，帮助后续维护者理解“当时为什么这样做”。

Devlog 不替代 `docs/`。`docs/` 记录当前对外行为、API、配置和工作流契约；devlog 记录一次 PR/主题内的设计决策过程。devlog 是当时决策记录，可能被后续 devlog 或 `Current` 文档 supersede；不要只凭单篇旧 devlog 判断当前契约。

## 什么时候需要写

以下变更应新增或更新 devlog：

- DB、Cache、K8s、Workflow、并发、安全、依赖或工具链变化
- 跨模块契约变化
- 存在有意义技术选型或方案取舍的实现，即使代码 diff 很小

以下变更通常可豁免，但需要在交付说明中说明原因：

- 纯内部小重构
- 测试、注释、格式、拼写修正
- 不涉及设计选择的小范围本地 bugfix

## 命名与组织

- 每个 PR/主题维护一篇 Markdown。
- 文件名使用 `YYYY-MM-DD-<topic-slug>.md`，例如 `2026-05-19-workflow-timeout-state.md`。
- 同一 PR 后续迭代更新同一篇 devlog，不为每次 commit 创建新文件。
- 新增或重命名 devlog 时，同步更新下面的索引。

## 索引

| 日期 | 文件 | 主题 |
| --- | --- | --- |
| 2026-09-02 | `2026-09-02-independent-open-source-baseline.md` | Eruun 独立开源产品基线与全新安装边界 |
| 2026-08-09 | `2026-08-09-adopted-recreation-recovery.md` | Adopted 资源重建的可恢复持久化协议 |
| 2026-08-08 | `2026-08-08-statefulset-immutable-rebuild-safety.md` | StatefulSet 不可变字段显式重建安全边界 |
| 2026-07-17 | `2026-07-17-application-status-consistency.md` | 应用状态读取、聚合与 Informer 写入一致性 |
| 2026-07-10 | `2026-07-10-job-failure-policy-opt-out.md` | Job 级失败清理例外 |
| 2026-07-08 | `2026-07-08-workflow-cleanup-pvc-retention.md` | Workflow cleanup 保留 standalone PVC |
| 2026-07-08 | `2026-07-08-create-exec-deploying-status.md` | 创建并执行初次部署状态 |
| 2026-07-08 | `2026-07-08-version-update-app-status.md` | 版本更新期间应用聚合状态 |
| 2026-07-06 | `2026-07-06-version-execution-scope.md` | 版本更新 workflow 执行范围 |
| 2026-07-03 | `2026-07-03-version-update-workload-ready.md` | 版本更新 workload Ready 观测 |
| 2026-07-03 | `2026-07-03-deployment-update-volume-mount-replace.md` | Deployment 更新替换 VolumeMount |
| 2026-06-29 | `2026-06-29-workflow-image-pull-policy.md` | Workflow 镜像拉取策略默认值 |
| 2026-06-23 | `2026-06-23-api-payload-aliases.md` | API payload 字段别名兼容 |
| 2026-06-23 | `2026-06-23-lifecycle-api-routes.md` | 生命周期 API 路由语义收敛 |
| 2026-06-22 | `2026-06-22-log-archive-upload-jobtype.md` | 日志归档同步下载与 workflow jobType |
| 2026-06-22 | `2026-06-22-application-resources-response.md` | 应用资源摘要响应结构 |
| 2026-06-17 | `2026-06-17-programming-language-crud.md` | 编程语言 CRUD API |
| 2026-06-16 | `2026-06-16-template-resource-summary.md` | 模板列表主组件资源摘要 |
| 2026-06-12 | `2026-06-12-version-update-task-callback.md` | 版本更新 task 级 callback |
| 2026-06-08 | `2026-06-08-ingress-nginx-tcp-dependencies.md` | ingress-nginx TCP 暴露 Redis/MySQL |
| 2026-06-01 | `2026-06-01-conversion-best-effort-contract.md` | Conversion best-effort 契约 |
| 2026-05-28 | `2026-05-28-domain-service-reorg.md` | Domain service 目录重组 |

## 模板

```markdown
# <主题>

## 背景与需求

说明问题来源、用户目标、现有行为和期望结果。

## 影响范围

- API:
- Domain:
- DB:
- Cache:
- K8s:
- Workflow:

## 技术选型与取舍

记录候选方案、最终选择、选择原因，以及明确放弃的方案和原因。

## 实现摘要

说明核心实现路径、关键文件或模块、重要契约变化。

## 测试与验收

记录已执行的测试命令、手工验证步骤、预期行为和验收口径。

## 风险与后续

记录已知风险、未覆盖场景、后续可改进方向。
```
