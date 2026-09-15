# gRPC v1 公共 API

> 状态：Current。本文描述与 HTTP `/api/v1` 并行的 Eruun 用户业务 gRPC 入口；以 `proto/eruun/v1/` 为字段、服务和流式契约的最终依据。

## 启动与部署

只有 `api` 角色开放 gRPC。直接启动服务器时，HTTP 默认监听 `127.0.0.1:8001`，gRPC 默认监听 `127.0.0.1:9001`。可通过 `--grpc-bind-addr` 或 `ERUUN_GRPC_BIND_ADDR` 指定独立地址；监听冲突或监听失败会使进程启动失败。Controller、Scheduler 和 Worker 不开放业务 gRPC，仍保留原有 HTTP 健康入口。

Helm 和静态清单让 API Pod 监听 `0.0.0.0:9000`，现有 ClusterIP Service 增加名为 `grpc` 的 9000 端口。HTTP 8000 端口、Ingress 与健康探针保持原样。进程不配置 TLS 或 mTLS；本地回环或可信集群内部可以直接连接，集群外使用时应由部署方配置支持 HTTP/2 的 TLS 网关，不应把明文 gRPC 端口直接暴露到不可信网络。

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

Protobuf 采用 `eruun.v1` 包；Go 客户端与服务端生成代码随仓库提交。`optional` 标量用于区分“未提供”与零值；动态 JSON 取值只在原有动态字段使用 `google.protobuf.Struct/Value`，规范 JSON Schema 原文为 `bytes`。固定生成工具为 protoc 35.0、protoc-gen-go v1.36.12、protoc-gen-go-grpc v1.6.2：

```bash
make grpc-gen
make grpc-gen-check
```

Workflow 步骤及子步骤的 `properties` 是重复字段，可按顺序提交多个属性条目；`GetApplicationSpec` 和 `ListApplicationWorkflows` 返回的可编辑 `spec` 也保留完整数组。更新已有 Workflow 时，省略 `failure_policy` 会保留现有策略，显式提交空字符串会重置为默认 `cleanup_all`。数据库重置的 `init_sqlurl` 仅在省略时视为未提供；显式空字符串是无效 URL，调用会失败且不会创建任务。独立 `command` Job 的 `traits.security_policy` 可设置容器安全上下文，提交和读取 Job 时均保留该字段。

## HTTP 路由到 RPC 对照

当前 HTTP 路由共 108 条；以下 99 条用户可调用路由各映射一个 RPC。表中全名与生成客户端方法一一对应，具体字段及请求/响应类型见各 `.proto` 文件。

| HTTP 路由 | gRPC 方法全名 |
| --- | --- |
| `DELETE /api/v1/applications/:appID` | `/eruun.v1.ApplicationService/DeleteApplication` |
| `DELETE /api/v1/applications/:appID/resources` | `/eruun.v1.ApplicationService/ApplyApplicationResourceCleanup` |
| `DELETE /api/v1/applications/:appID/workflow/schedule/:workflowID` | `/eruun.v1.ApplicationService/DeleteWorkflowSchedule` |
| `DELETE /api/v1/auth/identities/:identityID` | `/eruun.v1.AccountService/UnbindIdentity` |
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
| `GET /api/v1/job-storage-policy` | `/eruun.v1.JobsService/GetJobStoragePolicy` |
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
| `PUT /api/v1/job-storage-policy` | `/eruun.v1.JobsService/SetJobStoragePolicy` |
| `PUT /api/v1/jobs/:taskID/retention` | `/eruun.v1.JobsService/SetJobRetention` |
| `PUT /api/v1/programming-languages/:id` | `/eruun.v1.ProgrammingLanguagesService/UpdateProgrammingLanguage` |
| `PUT /api/v1/settings/:type` | `/eruun.v1.SettingsService/UpdateSetting` |

不映射的 9 条路由是四条 HTTP 健康路由（`/health`、`/healthz`、`/ready`、`/readyz`）、三条内部 Job Runner 回调（`/job-runners/:taskID/dataset`、`/results`、`/events`）以及两条浏览器 OAuth start/callback。该入口不涉及角色间通信、Runner 协议或 Agent/模型工作负载调用；现有 HTTP JSON 与 Cookie 契约不变。
