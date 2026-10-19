# 删除第三套定时机制、只写设置与 PaaS 遗留能力线（O60–O65）

## 背景

2026-10-19 的过度设计审计（基线 `origin/main@6a2f2d9`）确认六处与 Eruun 当前定位不符、或没有运行时效果的设计。删除分两个 PR 交付：第一个处理运行时内的重复与无效契约（O60–O63），第二个删除 PaaS 时代的能力线（O64–O65）。Eruun 尚未发布，两者都不保留兼容路径。

## 第一部分：O60–O63

### O60 `scheduledjob` 组件

Eruun 有三套定时机制：应用 Workflow 调度（`WorkflowSchedule`）、可复用 Job 定义的 cron 触发（`JobDefinition.cron`），以及把 `scheduledjob` 组件渲染成 Kubernetes CronJob。前两者由 Leader 按数据库到期时间派发，每次运行都进入 `WorkflowQueue`/`JobInfo` 状态机；CronJob 之后的每次触发由 kube-controller-manager 直接创建 Job，不经过准入、配额、取消和结果回写，违背“MySQL 状态是事实来源”。这条能力线却分布在约 25 个生产文件里（取消恢复、清理、结果身份戳、共享标签、Pod 日志、命名、资源名校验等）。

删除内容：`scheduledjob` 组件类型与 `scheduled_job` Job 类型、CronJob 生成/清理/取消恢复、`GET /cronjobs`（列出 namespace 内全部原生 CronJob，包括非 Eruun 创建的）和 `GET /scheduledjobs` 及对应 RPC、组件 properties 的 `schedule`/`successfulJobsHistoryLimit`/`failedJobsHistoryLimit`，以及运行时 ClusterRole 和空间 Role 中的 `batch/cronjobs` 权限。一次性延迟执行继续使用 `job` 组件的 `startTime`；周期执行使用 Workflow 调度或 Job 定义。

延迟队列中缺省 `jobType` 的载荷改按 `instant_job` 处理：当前唯一会写入延迟载荷的控制器就是即时 Job，且它总是显式写入 `jobType`。

### O61 只写不读的系统设置

`nodeSelector` 与 `rbacPolicies` 两类系统设置可以写入、读取，文档描述为“调度 nodeSelector”“RBAC policy”，但运行时没有任何读方；它们唯一的消费者是从更早的 `NodeSelectorProfile`/`RBACProfile` 表迁入数据的启动迁移。删除两种类型、对应模型、迁移分支、初始化 SQL 与示例。剩余的 `urlSecurityPolicy`、`podRestartMonitor`、`workflow_scheduler` 均由服务启动时自动补齐，`scripts/init-system-setting.sql` 因此一并删除。

`GET /settings` 改为只返回已注册类型：旧类型在库中的残留行、以及与设置共用一张表的 `accounts.bootstrap.v1` 标记都不再出现在列表里（此前只过滤了 `migration.` 前缀）。

### O62 无行为的 `workflowType`

`workflowType` 枚举中的 `test`、`scan`、`delivery` 只出现在常量、支持列表和 Schema 里，没有任何分支依赖它们，属于 CI/CD 流水线时代的分类。从枚举和 Schema 中删除。

### O63 日志归档

`POST /applications/:appID/log-archives` 与 `POST .../components/:componentName/files/export` 使用同一套 Pod 选择和打包逻辑，只多接受了不参与执行的 `name`/`mode`/`jobType`。异步的 `log_archive_upload` workflow jobType 依赖 `ArchiveUploader`，而生产代码从未调用 `SetArchiveUploader`，该 jobType 只会以“uploader 未配置”失败。删除路由、RPC、jobType、控制器，以及只为它存在的 workflow `properties[].path`/`properties[].container` 字段和按步骤名推断目标组件的回退。需要取日志文件时使用 `files/export`。

## 验证边界

本地 `go vet ./...`、`go test ./... -race`、gRPC 生成一致性检查通过。未在真实 Kubernetes 集群上验证 RBAC 收紧后的部署和清理路径。

## 未纳入

- 历史审计文档与旧 devlog 保留原文。
- 数据库中旧的 `scheduledjob` 组件行和 `nodeSelector`/`rbacPolicies` 设置行不会被自动清理；它们不再被读取，也不会出现在设置列表中。
