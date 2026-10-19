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

## 第二部分：O64–O65

### O64 PaaS 时代能力线

以下能力服务于早期 PaaS 的应用托管场景（数据库实例、语言运行时选型、应用模板复制），与 AI 工作负载运行时的定位无关，按 O55 的方式成组删除：

- **数据库重置**：`POST /applications/:appID/database-reset`、`database_reset` Job 类型与 workflowType、workflow `properties[].initSqlUrl`。它删除并重建 `store` 组件的 PVC，再通过 initContainer 的 `SQL_URL` 重新导入快照。
- **编程语言选项**：`/programming-languages` CRUD 与 `ProgrammingLanguagesService`。文档已注明它不参与应用创建、模板克隆或 Workflow 执行，只是一张配置表。
- **应用模板**：组件 `tmp`（模板引用）、应用 `templateEnabled`、`GET /applications/templates`、`POST /applications/:appID/version/diff-update`。模板克隆需要改写组件名、Service DNS、存储身份与选择器，约 1.5k 行生产代码与 3k 行测试；它还让应用名唯一性、资源名冲突校验和版本更新各自多出一条模板分支。
- **YAML 转换**：`POST /applications/convert` 把原生 Kubernetes YAML 尽力转换为组件，并支持按 URL 拉取，属于存量迁移工具，与已删除的存量导入能力线同源。

删除模板后，应用名在同一 namespace 内唯一，不再按“名称 + 版本”区分模板；`resolveComponents` 只保留空间边界校验。

### O65 cloudjob 与阿里云

`cloudjob` 组件为唯一的 `aliyun` Provider 搭了全局注册表、`CloudProvider`/`CloudRuntime`/`CloudAction` 三层接口、合约包和系统设置插件接口（`CloudProviderSettingSupport`），只提供 NAS 文件系统、挂载点和 StorageClass 三个动作。而空间授权会拒绝 `cloudjob` 组件类型和 `properties.cloud`，所有业务 API 又都要求空间作用域，因此该能力在业务 API 下不可达。

删除内容：`cloudjob` 组件与 `cloudjob_deploy` Job 类型、`properties.cloud`、Provider 注册与阿里云实现、`aliyunCloud` 系统设置及其连通性检查与脱敏、运行时 ClusterRole 中的 `storage.k8s.io/storageclasses` 权限，以及 12 个阿里云 SDK 相关依赖。短信验证码使用手写签名的 HTTPS 请求，不依赖这些 SDK，保持不变。

默认阶段化工作流去掉 `cloudjob` 阶段后重新编号为 `phase-1-config-secret`、`phase-2-store`、`phase-3-job`、`phase-4-webservice`。这正是此前作为“旧名称别名”保留的四个名字，别名表随之删除；仍以 `phase-2-config-secret` 等旧编号保存的默认工作流不再被识别为阶段化工作流，版本更新时不会自动同步其组件。

## 验证边界

两部分各自通过本地 `go vet ./...`、`go test ./... -race`、gRPC 生成一致性检查、Helm 模板测试与安装脚本测试。未在真实 MySQL 与 Kubernetes 集群上验证 RBAC 收紧后的部署、清理和默认工作流同步。

## 未纳入

- 历史审计文档与旧 devlog 保留原文。
- 数据库中旧的 `scheduledjob`/`cloudjob` 组件行、`nodeSelector`/`rbacPolicies`/`aliyunCloud` 设置行、`eruun_programming_languages` 表和 `eruun_applications.tmp_enable` 列不会被自动清理；它们不再被读取，设置残留行也不会出现在列表中。
