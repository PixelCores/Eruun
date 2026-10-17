# gRPC v1 公共 API

> 状态：Current。本文描述与 HTTP `/api/v1` 并行的 Eruun 用户业务 gRPC 入口；以 `pkg/apiserver/interfaces/grpc/proto/eruun/v1/` 为字段、服务和流式契约的最终依据。

## 启动与部署

只有当前 Leader 提供业务 gRPC；所有节点具备接任能力。HTTP 本地默认 `127.0.0.1:8001`，gRPC 默认 `127.0.0.1:9001`，通过 `--grpc-bind-addr` / `ERUUN_GRPC_BIND_ADDR` 配置。集群经固定 Leader Service 的 gRPC 端口访问，Worker 只提供健康入口。切主会取消旧任期 API 并关闭该任期的连接（含空闲连接）；Service 路由尚未更新时，Worker 也会断开新 gRPC 连接，使客户端继续重连。客户端重连后先核对任务状态，遵循具体 RPC 的幂等契约，不能自动重放所有写入。

Helm 和静态清单让当前 Leader 提供 `0.0.0.0:9000`，固定 Leader ClusterIP Service 提供名为 `grpc` 的 9000 端口。HTTP 8000 端口、Ingress 与健康探针保持原样。进程不配置 TLS 或 mTLS；本地回环或可信集群内部可以直接连接，集群外使用时应由部署方配置支持 HTTP/2 的 TLS 网关，不应把明文 gRPC 端口直接暴露到不可信网络。

## 认证与权限

注册和登录的原生 `LoginResponse` 返回 `access_token` 与 `refresh_token`。刷新使用 `AccountService.Refresh` 的 `RefreshRequest.refresh_token` 显式提交旧 refresh token，并返回轮换后的新令牌；客户端应安全保存令牌、串行刷新、覆盖旧值。现有 HTTP HttpOnly Cookie 行为不变。浏览器 OAuth start/callback 仍仅支持 HTTP。

受保护 RPC 通过 gRPC metadata 传递 `authorization: Bearer <access-token>`。空间操作另用 `x-eruun-workspace-id: <workspace-id>`；有幂等支持的操作可携带 `idempotency-key: <key>`。服务端复用账号会话、空间角色、资源归属和 HTTP 的操作类别限流；未知 RPC 默认拒绝。系统管理员权限不因请求元数据而提升。不要把令牌、密码或请求内容写入日志。

失败使用标准 gRPC status；`google.rpc.ErrorInfo` 的 metadata 中包含 `business_code`，文案只使用审核过的安全消息。HTTP 某些 `200 + 业务错误码` 响应在 gRPC 中是失败 status；业务部分成功仍返回强类型成功响应。客户端应以 status code 做分支，以业务 code 做更细的业务处理，而非解析 message 文本。

## 分块与取消

`JobsService.UploadJobDataset` 是客户端流，首个 `UploadDatasetPart` 提交元数据，其余分块提交压缩包数据；单块上限 64 KiB，压缩包总上限 64 MiB，上传截止 300 秒。`JobsService.DownloadJobResult`、`DownloadJobDelivery`、`DownloadJobDataset` 及组件文件/日志归档导出为服务端文件流。`ApplicationService.StreamComponentLogs` 与 `StreamComponentShell` 为服务端事件流，事件内容用 `bytes` 保持原始字节。普通单次请求的服务器接收上限为 24 MiB。取消 RPC 会关闭底层 reader、临时文件和执行资源；调用方也应使用有界 context。

## Go 客户端示例

以下示例仅用于本地回环明文连接；远程连接应使用 TLS 凭据和网关地址。

```go
package main

import (
    "context"
    "time"

    eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
    "google.golang.org/protobuf/types/known/emptypb"
)

func main() {
    conn, err := grpc.NewClient("127.0.0.1:9001",
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil { panic(err) }
    defer conn.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    token := "<access-token>" // 从安全存储读取，不打印
    workspaceID := "<workspace-id>"
    ctx = metadata.AppendToOutgoingContext(ctx,
        "authorization", "Bearer "+token,
        "x-eruun-workspace-id", workspaceID)
    client := eruunv1.NewAccountServiceClient(conn)
    _, err = client.GetMe(ctx, &emptypb.Empty{})
    if err != nil { panic(err) }
}
```

Protobuf 源文件放在 `pkg/apiserver/interfaces/grpc/proto/eruun/v1`，生成的 Go 客户端与服务端代码放在同一接口层的 `pb/v1` 并随仓库提交。生成时以 `pkg/apiserver/interfaces/grpc/proto` 为 include 根目录，Proto import 路径仍是 `eruun/v1/*.proto`，协议包名仍是 `eruun.v1`。`optional` 标量用于区分“未提供”与零值；动态 JSON 取值只在原有动态字段使用 `google.protobuf.Struct/Value`，规范 JSON Schema 原文为 `bytes`。固定生成工具为 protoc 35.0、protoc-gen-go v1.36.12、protoc-gen-go-grpc v1.6.2：

```bash
make grpc-gen
make grpc-gen-check
```

Workflow 步骤及子步骤的 `properties` 是重复字段，可按顺序提交多个属性条目；`GetApplicationSpec` 和 `ListApplicationWorkflows` 返回的可编辑 `spec` 也保留完整数组。更新已有 Workflow 时，省略 `failure_policy` 会保留现有策略，显式提交空字符串会重置为默认 `cleanup_all`。数据库重置的 `init_sqlurl` 仅在省略时视为未提供；显式空字符串是无效 URL，调用会失败且不会创建任务。独立 `command` step 的 `stages[].steps[].traits.security_policy` 可设置容器安全上下文，提交和读取 Job 时均保留该字段。

## LLM 测评声明与结果选择

独立 `SubmitJobRequest` 使用 `workspace_id`、`name`、可选 `volume`、`stages` 和可选 `execute_at`（Unix 秒）；每个 `JobStage` 包含 `name` 和 `steps`。`JobStep` 包含 `name`、`type`、`spec`、`traits` 和可选 `retry.backoff_limit`（0–10）；`traits` 直接复用 applications.proto 的 `AppSpecTraits`。command step 提交 `type: "command"` 和 `spec`，支持 `retry.backoff_limit`；评测 step 提交 `type: "job"`，将测评配置放在 `traits.eval` 并省略 `spec` 和 `retry`。评测与 Application 组件共享 `EvaluationTrait`，包括 `env`、`model`、`agent`、`task_package_id`、试验次数、并发、超时、Sandbox 资源及结果保存策略。

这是破坏性变更：原 `SubmitJobRequest` 顶层 `type/spec/traits` 已删除，连同更早移除的 `result_policy` 一起保留字段号并拒绝旧二进制请求；不提供兼容路径。`GetJob` 的 `JobSpec` 同步返回 stages，`JobAccepted.type` 统一为 `job`。`volume` 字段为 `size/storage_class/access_mode`，只挂载 command steps 到 `/workspace`；stage 并行与卷约束、trait 附属资源拒绝规则和终态清理与 HTTP 一致。框架版本由平台固定，`GetJob.framework_version` 返回所选执行实际冻结的版本。字段示例及约束见 [Job API](workspace-jobs-api.md)。

同一 Application Workflow 可以包含多个测评 Job。调用 `GetJob`、`GetJobResults`、结果/投递下载、投递重试和保留期更新时，Application 调用方必须同时提供父 `task_id` 与目标 Job 的 `execution_key`；响应中的 Job 明细、结果和投递也返回该标识。独立 Job 仅在评测执行唯一时可以省略 `execution_key`；同一 task 存在多个评测执行时必须显式指定。结果按执行分别保存，不能用一个执行的标识读取同一 Workflow 中另一个执行的制品。`CancelJob` 仅接受独立 Job 的父 `task_id`，不接受 `execution_key`；Application 仍通过 Workflow 取消接口取消任务。

## Job 定义与手动、延迟触发

`CreateJobDefinition` 直接接收 `JobSpec`；`UpdateJobDefinition` 接收 `definition_id` 和完整的 `spec`，整体替换成功后 `revision` 加 1。`GetJobDefinition`、`DeleteJobDefinition` 使用 `JobDefinitionRequest.definition_id`。定义响应包含 `id/name/revision/spec/creator/create_time/update_time`，不包含 cron 或并发策略。删除只删除定义，已有运行及其定义 ID、版本和规格快照保留。

`RunJobDefinition` 接收 `definition_id` 和可选 `execute_at`，返回与 `SubmitJob` 相同的 `JobAccepted`。两者的 `execute_at` 均为 Unix 秒：负数返回 `InvalidArgument`，省略、0 或过去的时间立即可派发，未来时间保持 `waiting` 直到到期。定义触发把当时的规格与版本写入运行，后续替换定义不改变已入队的运行。

`ListJobDefinitions` 和 `ListJobs` 使用可选 `page/page_size`，省略时为 1/20，`page` 范围 1–10000000，`page_size` 范围 1–100；响应包含 `items/total/page/page_size`。`ListJobs` 只列当前空间的独立运行，按创建时间倒序；可选 `definition_id` 过滤也适用于已删除定义留下的运行。每项 `JobRun` 包含 `accepted`、`name`、`job_definition_id`、`job_definition_revision`、`execute_at`、创建者和时间。读需要 viewer，创建、替换、删除和触发需要 member，空间通过现有 metadata 选择。

## HTTP 路由到 RPC 对照

当前 HTTP 路由共 121 条；以下 106 条用户可调用路由各映射一个 RPC。表中全名与生成客户端方法一一对应，具体字段及请求/响应类型见各 `.proto` 文件。

| HTTP 路由 | gRPC 方法全名 |
| --- | --- |
| `DELETE /api/v1/applications/:appID` | `/eruun.v1.ApplicationService/DeleteApplication` |
| `DELETE /api/v1/applications/:appID/resources` | `/eruun.v1.ApplicationService/ApplyApplicationResourceCleanup` |
| `DELETE /api/v1/applications/:appID/workflow/schedule/:workflowID` | `/eruun.v1.ApplicationService/DeleteWorkflowSchedule` |
| `DELETE /api/v1/auth/identities/:identityID` | `/eruun.v1.AccountService/UnbindIdentity` |
| `DELETE /api/v1/job-definitions/:definitionID` | `/eruun.v1.JobsService/DeleteJobDefinition` |
| `DELETE /api/v1/programming-languages/:id` | `/eruun.v1.ProgrammingLanguagesService/DeleteProgrammingLanguage` |
| `DELETE /api/v1/settings/:type` | `/eruun.v1.SettingsService/DeleteSetting` |
| `DELETE /api/v1/workspaces/:workspaceID` | `/eruun.v1.AccountService/DeleteWorkspace` |
| `DELETE /api/v1/workspaces/:workspaceID/invitations/:invitationID` | `/eruun.v1.AccountService/RevokeWorkspaceInvitation` |
| `DELETE /api/v1/workspaces/:workspaceID/members/:userID` | `/eruun.v1.AccountService/RemoveWorkspaceMember` |
| `GET /api/v1/admin/users` | `/eruun.v1.AccountService/ListAdminUsers` |
| `GET /api/v1/applications` | `/eruun.v1.ApplicationService/ListApplications` |
| `GET /api/v1/applications/:appID/components` | `/eruun.v1.ApplicationService/ListApplicationComponents` |
| `GET /api/v1/applications/:appID/components/:componentName/containers` | `/eruun.v1.ApplicationService/ListComponentContainers` |
| `GET /api/v1/applications/:appID/components/:componentName/logs` | `/eruun.v1.ApplicationService/StreamComponentLogs` |
| `GET /api/v1/applications/:appID/components/status` | `/eruun.v1.ApplicationService/GetApplicationComponentStatus` |
| `GET /api/v1/applications/:appID/spec` | `/eruun.v1.ApplicationService/GetApplicationSpec` |
| `GET /api/v1/applications/:appID/status` | `/eruun.v1.ApplicationService/GetApplicationStatus` |
| `GET /api/v1/applications/:appID/workflow/schedules` | `/eruun.v1.ApplicationService/ListWorkflowSchedules` |
| `GET /api/v1/applications/:appID/workflow/tasks` | `/eruun.v1.ApplicationService/ListApplicationTasks` |
| `GET /api/v1/applications/:appID/workflows` | `/eruun.v1.ApplicationService/ListApplicationWorkflows` |
| `GET /api/v1/applications/templates` | `/eruun.v1.ApplicationService/ListTemplateApplications` |
| `GET /api/v1/auth/identities` | `/eruun.v1.AccountService/ListIdentities` |
| `GET /api/v1/auth/me` | `/eruun.v1.AccountService/GetMe` |
| `GET /api/v1/auth/methods` | `/eruun.v1.AccountService/GetAuthMethods` |
| `GET /api/v1/cronjobs` | `/eruun.v1.ApplicationService/ListCronJobs` |
| `GET /api/v1/job-datasets` | `/eruun.v1.JobsService/ListJobDatasets` |
| `GET /api/v1/job-datasets/:datasetID` | `/eruun.v1.JobsService/GetJobDataset` |
| `GET /api/v1/job-datasets/:datasetID/download` | `/eruun.v1.JobsService/DownloadJobDataset` |
| `GET /api/v1/job-definitions` | `/eruun.v1.JobsService/ListJobDefinitions` |
| `GET /api/v1/job-definitions/:definitionID` | `/eruun.v1.JobsService/GetJobDefinition` |
| `GET /api/v1/job-storage-policy` | `/eruun.v1.JobsService/GetJobStoragePolicy` |
| `GET /api/v1/jobs` | `/eruun.v1.JobsService/ListJobs` |
| `GET /api/v1/jobs/:taskID` | `/eruun.v1.JobsService/GetJob` |
| `GET /api/v1/jobs/:taskID/deliveries/:target/download` | `/eruun.v1.JobsService/DownloadJobDelivery` |
| `GET /api/v1/jobs/:taskID/results` | `/eruun.v1.JobsService/GetJobResults` |
| `GET /api/v1/jobs/:taskID/results/:artifactID/download` | `/eruun.v1.JobsService/DownloadJobResult` |
| `GET /api/v1/programming-languages` | `/eruun.v1.ProgrammingLanguagesService/ListProgrammingLanguages` |
| `GET /api/v1/programming-languages/:id` | `/eruun.v1.ProgrammingLanguagesService/GetProgrammingLanguage` |
| `GET /api/v1/resource-import/jobs/:taskID` | `/eruun.v1.ResourceImportService/GetResourceImportJob` |
| `GET /api/v1/scheduledjobs` | `/eruun.v1.ApplicationService/ListScheduledJobs` |
| `GET /api/v1/schemas/v1/canonical.json` | `/eruun.v1.ApplicationService/GetCanonicalJSONSchema` |
| `GET /api/v1/settings` | `/eruun.v1.SettingsService/ListSettings` |
| `GET /api/v1/settings/:type` | `/eruun.v1.SettingsService/GetSetting` |
| `GET /api/v1/workflow/tasks/:taskID/stages` | `/eruun.v1.ApplicationService/GetWorkflowTaskStages` |
| `GET /api/v1/workflow/tasks/:taskID/status` | `/eruun.v1.ApplicationService/GetWorkflowTaskStatus` |
| `GET /api/v1/workspaces` | `/eruun.v1.AccountService/ListWorkspaces` |
| `GET /api/v1/workspaces/:workspaceID` | `/eruun.v1.AccountService/GetWorkspace` |
| `GET /api/v1/workspaces/:workspaceID/members` | `/eruun.v1.AccountService/ListWorkspaceMembers` |
| `PATCH /api/v1/admin/users/:userID` | `/eruun.v1.AccountService/SetAdminUserDisabled` |
| `PATCH /api/v1/workspaces/:workspaceID` | `/eruun.v1.AccountService/RenameWorkspace` |
| `PATCH /api/v1/workspaces/:workspaceID/members/:userID` | `/eruun.v1.AccountService/UpdateWorkspaceMember` |
| `POST /api/v1/applications` | `/eruun.v1.ApplicationService/CreateApplications` |
| `POST /api/v1/applications/:appID/components/:componentName/files/export` | `/eruun.v1.ApplicationService/ExportComponentFiles` |
| `POST /api/v1/applications/:appID/components/:componentName/shell/exec` | `/eruun.v1.ApplicationService/ExecComponentShell` |
| `POST /api/v1/applications/:appID/components/:componentName/shell/stream` | `/eruun.v1.ApplicationService/StreamComponentShell` |
| `POST /api/v1/applications/:appID/database-reset` | `/eruun.v1.ApplicationService/ResetApplicationDatabases` |
| `POST /api/v1/applications/:appID/log-archives` | `/eruun.v1.ApplicationService/DownloadLogArchive` |
| `POST /api/v1/applications/:appID/resources/cleanup-plan` | `/eruun.v1.ApplicationService/PlanApplicationResourceCleanup` |
| `POST /api/v1/applications/:appID/restart` | `/eruun.v1.ApplicationService/RestartApplicationWorkloads` |
| `POST /api/v1/applications/:appID/start` | `/eruun.v1.ApplicationService/StartApplicationDeployments` |
| `POST /api/v1/applications/:appID/stop` | `/eruun.v1.ApplicationService/StopApplicationDeployments` |
| `POST /api/v1/applications/:appID/version` | `/eruun.v1.ApplicationService/UpdateApplicationVersion` |
| `POST /api/v1/applications/:appID/version/cancel` | `/eruun.v1.ApplicationService/CancelDelayedVersionUpdate` |
| `POST /api/v1/applications/:appID/version/diff-update` | `/eruun.v1.ApplicationService/DiffUpdateApplicationVersion` |
| `POST /api/v1/applications/:appID/workflow/cancel` | `/eruun.v1.ApplicationService/CancelApplicationWorkflow` |
| `POST /api/v1/applications/:appID/workflow/exec` | `/eruun.v1.ApplicationService/ExecApplicationWorkflow` |
| `POST /api/v1/applications/:appID/workflow/schedule` | `/eruun.v1.ApplicationService/UpsertWorkflowSchedule` |
| `POST /api/v1/applications/:appID/workflow/tasks/cancel-all` | `/eruun.v1.ApplicationService/CancelAllApplicationWorkflows` |
| `POST /api/v1/applications/:appID/workflow/try` | `/eruun.v1.ApplicationService/TryWorkflow` |
| `POST /api/v1/applications/components/status` | `/eruun.v1.ApplicationService/BatchApplicationComponentStatus` |
| `POST /api/v1/applications/convert` | `/eruun.v1.ApplicationService/ConvertApplications` |
| `POST /api/v1/applications/create-and-exec` | `/eruun.v1.ApplicationService/CreateAndExecApplications` |
| `POST /api/v1/applications/import/namespace` | `/eruun.v1.ApplicationService/ImportNamespaceApplications` |
| `POST /api/v1/applications/import/namespace/try` | `/eruun.v1.ApplicationService/TryImportNamespaceApplications` |
| `POST /api/v1/applications/query` | `/eruun.v1.ApplicationService/BatchGetApplications` |
| `POST /api/v1/applications/try` | `/eruun.v1.ApplicationService/TryApplication` |
| `POST /api/v1/auth/codes` | `/eruun.v1.AccountService/SendAuthCode` |
| `POST /api/v1/auth/identities` | `/eruun.v1.AccountService/BindIdentity` |
| `POST /api/v1/auth/login` | `/eruun.v1.AccountService/Login` |
| `POST /api/v1/auth/logout` | `/eruun.v1.AccountService/Logout` |
| `POST /api/v1/auth/password/reset` | `/eruun.v1.AccountService/ResetPassword` |
| `POST /api/v1/auth/refresh` | `/eruun.v1.AccountService/Refresh` |
| `POST /api/v1/auth/register` | `/eruun.v1.AccountService/Register` |
| `POST /api/v1/job-datasets` | `/eruun.v1.JobsService/UploadJobDataset` |
| `POST /api/v1/job-definitions` | `/eruun.v1.JobsService/CreateJobDefinition` |
| `POST /api/v1/job-definitions/:definitionID/runs` | `/eruun.v1.JobsService/RunJobDefinition` |
| `POST /api/v1/jobs` | `/eruun.v1.JobsService/SubmitJob` |
| `POST /api/v1/jobs/:taskID/cancel` | `/eruun.v1.JobsService/CancelJob` |
| `POST /api/v1/jobs/:taskID/deliveries/:target/retry` | `/eruun.v1.JobsService/RetryJobDelivery` |
| `POST /api/v1/programming-languages` | `/eruun.v1.ProgrammingLanguagesService/CreateProgrammingLanguage` |
| `POST /api/v1/resource-import/jobs/manage` | `/eruun.v1.ResourceImportService/SubmitResourceImportManage` |
| `POST /api/v1/resource-import/jobs/scan` | `/eruun.v1.ResourceImportService/SubmitResourceImportScan` |
| `POST /api/v1/settings` | `/eruun.v1.SettingsService/CreateSetting` |
| `POST /api/v1/workflow/tasks/:taskID/approval` | `/eruun.v1.ApplicationService/ApproveWorkflowTask` |
| `POST /api/v1/workspace-invitations/accept` | `/eruun.v1.AccountService/AcceptWorkspaceInvitation` |
| `POST /api/v1/workspaces` | `/eruun.v1.AccountService/CreateWorkspace` |
| `POST /api/v1/workspaces/:workspaceID/invitations` | `/eruun.v1.AccountService/InviteWorkspaceMember` |
| `POST /api/v1/workspaces/:workspaceID/transfer` | `/eruun.v1.AccountService/TransferWorkspace` |
| `PUT /api/v1/applications/:appID/workflow` | `/eruun.v1.ApplicationService/UpdateApplicationWorkflow` |
| `PUT /api/v1/auth/password` | `/eruun.v1.AccountService/ChangePassword` |
| `PUT /api/v1/job-definitions/:definitionID` | `/eruun.v1.JobsService/UpdateJobDefinition` |
| `PUT /api/v1/job-storage-policy` | `/eruun.v1.JobsService/SetJobStoragePolicy` |
| `PUT /api/v1/jobs/:taskID/retention` | `/eruun.v1.JobsService/SetJobRetention` |
| `PUT /api/v1/programming-languages/:id` | `/eruun.v1.ProgrammingLanguagesService/UpdateProgrammingLanguage` |
| `PUT /api/v1/settings/:type` | `/eruun.v1.SettingsService/UpdateSetting` |

不映射的 15 条路由是四条 HTTP 健康路由（`/health`、`/healthz`、`/ready`、`/readyz`）、九条内部 Job Runner 接口（dataset/results/events、Sandbox 申请/查询/释放，以及 Checkpoint 上传/查询/材料下载）以及两条浏览器 OAuth start/callback。该入口不涉及角色间通信、Runner 协议或 Agent/模型工作负载调用；现有 HTTP JSON 与 Cookie 契约不变。


`JobsService/GetJob` 的 `JobDetail.sandboxes` 为所选 execution 的安全 Sandbox 生命周期投影，最多 100 条，优先仍占资源的记录；`sandboxes_truncated=true` 表示历史未全部返回。HTTP 对应 `sandboxesTruncated`，两者不提供完整事件审计或实际 trial 运行证明。

`EvaluationTrait.recovery` 同时用于 Workspace Job 和 Application 组件，字段为 `agent_version`、`replay_safe`、`checkpoint_interval_seconds`。版本和验证规则与 [HTTP 恢复契约](harbor-runtime-recovery.md) 一致；三个 Checkpoint Runner 内部 HTTP 路由不提供用户 RPC。
