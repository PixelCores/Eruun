# 空间 Job 与 Harbor 评测 API

> 状态：Current。描述本实现的后端行为。需求来源见 [需求基线](workspace-jobs-and-harbor-requirements.md)；更广的 Agent/MCP 能力仍属于 Proposal。

## 身份与提交

独立任务通过 `type` 区分 `command` 和 `agent_evaluation`。两种类型都在当前授权空间的 namespace 执行，平台生成 `taskId`，不要求 `appId`、应用组件或用户提供的 TaskID。执行复用 WorkflowQueue、JobInfo、现有调度和执行租约。

业务接口使用登录 Bearer Token 和 `X-Eruun-Workspace-ID`。读取需要空间成员权限，viewer 可读取；提交、上传、修改策略、取消及重试要求 member 或更高角色。创建请求的 `workspaceId` 必须与当前空间一致。

普通命令：

```json
{
  "workspaceId": "<当前空间 ID>",
  "name": "hello",
  "type": "command",
  "spec": {
    "image": "busybox:1.37.0",
    "command": ["sh", "-c"],
    "args": ["echo hello"],
    "timeoutSeconds": 300
  },
  "traits": {
    "securityPolicy": {"runAsUser": 1000, "runAsNonRoot": true},
    "resources": {"cpu": "100m", "memory": "128Mi", "cpuLimit": "500m", "memoryLimit": "256Mi"}
  }
}
```

提交到 `POST /api/v1/jobs`，返回 HTTP 202，响应 `data` 包含 `taskId`、`workspaceId`、`type`、`status`。名称不承担资源唯一性，相同名称可以重复提交。命令默认超时 3600 秒，范围 1–86400 秒；不支持 cron、延迟执行或失败自动重跑。普通命令支持资源、安全、环境变量、envFrom 及现有 PVC/ConfigMap/Secret 挂载；不创建附属存储、服务、RBAC 或应用实体。

## Harbor 评测 JSON

首版固定 Harbor **0.22.0**。上传原生任务包后，用返回的 `id` 提交：

```json
{
  "workspaceId": "<当前空间 ID>",
  "name": "agent-capability-evaluation",
  "type": "agent_evaluation",
  "spec": {
    "framework": "harbor",
    "frameworkVersion": "0.22.0",
    "datasetId": "<任务包上传返回的 ID>",
    "agent": {
      "name": "terminus-2",
      "model": "<Harbor 支持的 provider/model>",
      "credentials": [
        {"name": "OPENAI_API_KEY", "secretKeyRef": {"name": "evaluation-model", "key": "api-key"}}
      ]
    },
    "options": {"attempts": 1, "concurrency": 1},
    "timeoutSeconds": 3600
  },
  "traits": {
    "resources": {"cpu": "1", "memory": "2Gi", "cpuLimit": "2", "memoryLimit": "4Gi"}
  },
  "resultPolicy": {
    "retentionDays": 90,
    "targets": [
      {"type": "minio", "mode": "full"},
      {"type": "database", "mode": "metadata"}
    ]
  }
}
```

允许 `terminus-2`、`codex`、`claude-code` 和 `oracle`。`oracle` 执行任务包的参考解答，用于验证任务与平台链路，不代表模型能力；使用它时省略 `model` 和 `credentials`。其他 Agent 必须指定模型。支持的凭据环境名为 `OPENAI_API_KEY`、`ANTHROPIC_API_KEY`、`GEMINI_API_KEY`、`GOOGLE_API_KEY`、`OPENROUTER_API_KEY`、`AZURE_API_KEY`，均引用当前空间已有 Secret 的键；平台不返回 Secret 内容。

`attempts` 为每个任务的评测次数，默认 1，范围 1–10；`concurrency` 为 Harbor 同时执行的 trial 数，默认 1，范围 1–16。它们不改变 Eruun 的 Job 调度器并发策略。评测默认超时 3600 秒，范围 60–86400 秒，另预留 360 秒停止和归档时间；归档传输的总预算为 300 秒，普通 API 仍保留原来的超时限制。

评测只支持 `resources` Trait；省略时默认请求 1 CPU/2 GiB、限制 2 CPU/4 GiB。这组值应用于 Runner 和每个任务环境，实际总资源随并发数增加，仍受空间配额约束。框架参数位于 `spec`，不增加 Harbor Trait。不能提交任意 Python 适配器、环境 kwargs、命名空间、ServiceAccount 或 Pod 覆盖。

## 原生任务包

上传格式为 `application/gzip` 的 tar.gz，支持根目录单个任务或多个任务目录。每个任务包含 `instruction.md`、`task.toml`、`environment/Dockerfile`、`tests/test.sh`，以及所需输入文件和可选 `solution/solve.sh`。用户定义 verifier 和评分规则，平台保留其奖励、日志与输出。

`task.toml` 的 `[environment].docker_image` 必须引用显式标签或 digest 的预构建镜像。用户在本机构建并推送镜像，平台只拉取、执行，不构建 Dockerfile。任务包可重复用于不同 Job，不随某次结果到期删除。

```sh
tar -czf task-package.tar.gz -C examples/agent-evaluation harbor-task
curl -X POST "$ERUUN_URL/api/v1/job-datasets?name=harbor-demo" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "X-Eruun-Workspace-ID: $WORKSPACE_ID" \
  -H 'Content-Type: application/gzip' --data-binary @task-package.tar.gz
```

先按 [示例镜像说明](../examples/agent-evaluation/harbor-task/README.md) 构建并调整镜像引用，再打包。Task 包压缩上限 64 MiB、展开上限 256 MiB；结果分别为 512 MiB、2 GiB；最多 10,000 条目。拒绝路径逃逸、重复歧义条目、任务包链接及特殊文件。超过边界会明确失败，不宣称已完整采集。

## 结果与保存

`GET /api/v1/jobs/:taskID` 分别返回队列执行 `status`、`executions`、`collectionState`、`results` 和 `deliveries`。`collectionState` 为 `pending`、`collected`、`incomplete`、`unavailable` 或 `expired`。原始结果的 `summary.executionStatus` 保留框架事实，`summary.collectionComplete` 表示采集完整性。

原始结果是完整 tar.gz：根 `result.json` 描述采集；`outputs/` 保留 Harbor 的结果、trial、奖励、轨迹、日志、产物及其他文件。文件清单和摘要便于查询，下载仍提供完整归档。采集完整性同时检查试验 Pod 的下载和 Runner 本地归档，Harbor 内部吞掉的下载异常也会标记为不完整。原生失败、取消、采集失败和无法上传分别可辨认；reward 为 0 本身不是运行失败。

源数据被事务性保存后，各目标记录为 `pending`。Controller 自动执行保存，状态独立为 `pending/running/succeeded/failed`，失败记录 `lastError`。用户仅需重试失败目标；成功目标重复重试为幂等操作，不重复评测。相同源归档重复上传被接受，不同内容不可覆盖已发布源。

- `minio/full` 保存完整文件；bucket 必须由管理员预先创建。
- `database/full` 在平台已配置的数据库保存元数据和文件字节，采用 1 MiB 分块。
- `database/metadata` 保存索引和文件引用，必须同时选择 `minio/full`；MinIO 成功后才能完成此目标。

一个目标失败不会回滚其他成功目标或改变已经完成的评测事实。失败保存需要显式重试；进程重启中断的保存由数据库租约恢复。原始数据过期后，未成功的保存无法再用该来源重试，返回 HTTP 410。

## 策略与生命周期

注册时初始化个人空间默认策略；团队空间创建时同样初始化。已有空间未存策略时使用同一默认值。通过现有空间成员授权使用管理员提供的数据库和 MinIO 能力，不另建用户身份系统。

默认策略为 `retentionDays: 90` 和 `database/full`。`GET /api/v1/job-storage-policy` 返回可用目标及当前策略；`PUT` 使用与 `resultPolicy` 相同的 JSON 更新默认值，范围 1–3650 天。提交时快照化所选策略；省略 `resultPolicy` 使用空间默认值。修改默认策略只影响新任务。

原始数据保留期从采集成功开始计算。`PUT /api/v1/jobs/:taskID/retention` 接收 `{"retentionDays":30}`，将尚未过期的原始数据改为从本次操作起再保留 30 天。周期清理只删除源字节，保留可查询的过期元数据，**不删除 MinIO 或数据库保存副本**。每个 trial 的原始数据完整下载到 Runner 后可清理其试验 Pod；下载失败的试验 Pod 随未完整采集的 Runner Pod 保留供诊断，仍受执行截止时间约束，并随 Runner 的保留期限到期回收。最终归档或上传失败时，Runner 中已有的原始文件继续保留。

显式删除整个团队空间沿用“资源必须为空”的规则，且要求没有等待或正在保存的目标。空间删除会删除其数据库数据和索引；MinIO 已保存对象不由原始结果保留策略或空间删除 API 清除，需要存储管理员独立管理。

## 接口清单

| 方法与路径（均加 `/api/v1`） | 用途 |
| --- | --- |
| `POST /jobs` | 创建独立 Job，返回 202 |
| `GET /jobs/:taskID` | 类型、执行、采集及保存状态 |
| `POST /jobs/:taskID/cancel` | 取消已有任务；无需 AppID |
| `POST /job-datasets?name=...` | 上传原生任务 tar.gz，返回 201 |
| `GET /job-datasets?page=1&pageSize=20` | 列出空间任务包；pageSize 为 1–100 |
| `GET /job-datasets/:datasetID` | 任务包清单与摘要 |
| `GET /job-datasets/:datasetID/download` | 下载任务包 |
| `GET /jobs/:taskID/results` | 结果文件清单及各保存目标状态 |
| `GET /jobs/:taskID/results/:artifactID/download` | 下载原始数据或数据库完整副本 |
| `GET /jobs/:taskID/deliveries/:target/download` | 下载已成功保存的完整数据，包括 metadata 目标指向的 MinIO 文件 |
| `POST /jobs/:taskID/deliveries/:target/retry` | 单独重试 `minio` 或 `database` 保存，返回 202 |
| `PUT /jobs/:taskID/retention` | 修改已采集、未过期源数据的保留时间 |
| `GET/PUT /job-storage-policy` | 获取或修改空间默认策略 |

`/job-runners/:taskID/dataset` 和 `/job-runners/:taskID/results` 是内部传输接口：校验任务能力凭据、Pod UID、所属 Job UID、持久化执行代与恢复记录，登录 Token 不能代替 Runner 身份。旧执行者不能覆盖新执行结果。

## 管理员配置与部署

普通 `command` 不依赖 Harbor 配置。启用评测时，为所有运行角色设置 `--jobs-config-file` / `ERUUN_JOBS_CONFIG_FILE`，指向只读 Secret JSON：

```json
{
  "runnerImage": "registry.example.com/eruun-harbor-runner:0.22.0",
  "apiURL": "http://eruun-api.eruun-system.svc:8000",
  "runnerEgress": [
    {"cidr": "192.0.2.10/32", "port": 443},
    {"cidr": "192.0.2.11/32", "port": 6443},
    {"cidr": "192.0.2.12/32", "port": 8000}
  ],
  "minio": {
    "endpoint": "minio.eruun-system.svc:9000",
    "bucket": "evaluation-results",
    "accessKey": "<从 Secret 配置>",
    "secretKey": "<从 Secret 配置>",
    "secure": false,
    "region": "us-east-1"
  }
}
```

示例地址必须替换为实际地址。`runnerEgress` 仅允许单个 `/32` 或 `/128` 地址及端口，用于 Kubernetes API 和 Eruun API；根据 CNI 对 Service DNAT 的策略匹配方式，列出所需 Service IP 与后端地址。只对固定 Runner Pod 增加这些出站权限，任务环境沿用空间网络策略。`minio` 可省略，此时只提供数据库保存。

Runner 使用固定、无 Secret 读权限的空间 ServiceAccount，只获得 Pod 生命周期与 exec 能力。任务环境不挂载 API Token。空间仍强制 restricted Pod Security、UID 1000、禁止提权与 capabilities；见 [Runner 的镜像和适配边界](../runners/harbor/README.md)。

Helm 使用 `jobs.existingSecret` 和 `jobs.key` 挂载用户已创建的 Secret，Chart 不生成或公开存储凭据。四种角色需要相同配置；Controller 执行结果保存及过期清理，Worker 执行 Harbor Job。先升级数据库 schema，再升级各角色。提供的单文件安装清单不默认启用 Harbor；使用 Helm 或为清单各角色手动挂载同一 Secret。

## 验证

Go 测试覆盖类型校验、空间授权、Runner 执行身份和恢复、完整归档、并发发布、独立保存及过期。存储集成测试使用真实 MySQL 8.4 和 MinIO；运行方式：

```sh
go test -race -tags=integration ./pkg/apiserver/jobs/artifacts
```

需要 `MYSQL_TEST_DSN` 和 `MINIO_TEST_CONFIG` 指向隔离环境；未配置时集成部分跳过，不能作为真实数据库验证。Python 测试和本地镜像构建见 [Runner README](../runners/harbor/README.md)。不同付费模型、私有镜像仓库和生产 CNI 网络规则仍需部署方验证。
