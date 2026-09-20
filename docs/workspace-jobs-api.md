# 空间 Job 与 Harbor 评测 API

> 状态：Current。描述本实现的后端行为。需求来源见 [需求基线](workspace-jobs-and-harbor-requirements.md)；更广的 Agent/MCP 能力仍属于 Proposal。

## 身份与提交

普通命令使用 `type: command` 和 `spec`；LLM 评测使用 `type: job` 和 `traits.eval`。评测声明也可直接用于 Application 的 `type: job` 组件，两种入口共享规格、校验和 Harbor Runner。执行复用 WorkflowQueue、JobInfo、现有调度与执行租约。

独立 Job 由平台生成 `taskId`，不要求 `appId`，不创建占位应用。Application 内评测沿用所属 Workflow 的 AppID 和 TaskID，以各 Job 的 `executionKey` 区分配置、状态、结果与保存策略。

**契约迁移**：Trait 键由 `traits.evaluation` 改为 `traits.eval`，旧键不再接受；旧 `type: eval`、评测 `spec`、`framework/frameworkVersion`、`datasetId`、嵌套 `agent.model`、`options` 或顶层 `resultPolicy` 也不再接受。升级前排空或取消旧评测并确认 Kubernetes Job 已停止，导出所需历史结果，然后升级数据库 schema 和各运行角色。普通 `command` 请求不变。

业务接口使用登录 Bearer Token 和 `X-Eruun-Workspace-ID`。读取需要空间成员权限，viewer 可读取；提交、上传、修改策略、取消及重试要求 member 或更高角色。创建请求可省略 `workspaceId`，由已授权的请求空间决定；若显式填写，必须与该空间一致。空间 ID 在创建空间时生成，不会为每个 Job 新建空间。

普通命令：

```json
{
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

`traits.eval` 描述测评任务；模型是被比较的对象，`agent` 是执行任务的 harness。任务内容、镜像、参考解答与 verifier 保存在原生任务包里，DSL 不再复制 Harbor JobConfig。当前 Runner 使用 Harbor **0.22.0**，输入只声明 `env: ack`；不接受 framework 或框架版本字段。

```json
{
  "name": "model-benchmark",
  "type": "job",
  "traits": {
    "eval": {
      "env": "ack",
      "model": "<provider/model>",
      "agent": "terminus-2",
      "taskPackageId": "<任务包上传返回的 ID>",
      "attempts": 1,
      "concurrency": 1,
      "timeoutSeconds": 3600,
      "resultPolicy": {
        "retentionDays": 90,
        "targets": [
          {
            "type": "database",
            "mode": "full"
          }
        ]
      }
    },
    "resources": {
      "cpu": "1",
      "memory": "2Gi"
    },
    "envs": [
      {
        "name": "OPENAI_API_KEY",
        "valueFrom": {
          "secret": {
            "name": "evaluation-model",
            "key": "api-key"
          }
        }
      }
    ]
  }
}
```

上传任务包后，把响应 `data.id` 填入 `taskPackageId`。这个 ID 是 Eruun 任务包归档句柄，不是 Harbor 的数据集名称、版本或原生 task ID。Runner 下载归档，将解包后的本地路径交给 Harbor；当前不支持直接引用 Harbor 注册数据集。相同任务包可供多个评测复用。

允许 `terminus-2`、`codex`、`claude-code` 和 `oracle`。`agent` 指定 Harbor 执行 trial 的 harness；`oracle` 执行任务包的参考解答，用于验证任务与平台链路，不代表模型能力；使用它时省略 `model`，无需模型调用或模型费用。其他 Agent 必须指定模型。模型凭据通过 `traits.envs` 传入，支持的环境名为 `OPENAI_API_KEY`、`ANTHROPIC_API_KEY`、`GEMINI_API_KEY`、`GOOGLE_API_KEY`、`OPENROUTER_API_KEY`、`AZURE_API_KEY`，必须使用 `valueFrom.secret` 引用当前空间已有 Secret 的键；不接受 `valueFrom.static` 明文值，平台不返回 Secret 内容。

`attempts` 为每个任务的评测次数，默认 1，范围 1–10；`concurrency` 为 Harbor 同时执行的 trial 数，默认 1，范围 1–16。它们不改变 Eruun 的 Job 调度器并发策略。评测默认超时 3600 秒，支持 60–1209600 秒（14 天），并受管理员在线设置的 `workflow_scheduler.maxEvaluationTimeoutSeconds` 约束。降低上限不会改变已运行执行的恢复期限。Harbor 任务自身的 agent/verifier 时限仍由任务作者设置，平台外层时长不会自动延长这些时限。

新 Runner 另预留 960 秒收尾窗口，包含 600 秒控制面中断预算和原有 360 秒正常处理余量。任务包下载预算仍为 300 秒；进入 finalizing 后采集、归档、上传和 terminal 确认分享同一个有界窗口，按段截止并为确认保留最低余额。上传按 deadline 重试同一归档，不重新生成不同 digest；临时失败采用有上限的退避和抖动，单次 HTTP 发送、响应头与响应体读取受期限约束。Python 标准库 DNS 解析存在不能被 socket 截止直接打断的边界，须在部署中验证 DNS 稳定性。terminal 就绪后优先于未完成的旧心跳/进度通知，使用更高 sequence；claim 始终是执行门禁。窗口耗尽只能报告未确认交付，不能把已上传但 ACK 丢失当作尚未上传。显式旧 360 秒配置仍可读取；单次网络和 HTTP 入口超时仍分别生效，大制品应实测吞吐和整个收尾预算。

`traits.resources` 只控制 Runner；`traits.eval.sandboxResources` 单独控制每个 trial 的任务环境。两者各自省略时默认请求 1 CPU/2 GiB、限制 2 CPU/4 GiB，总资源随 concurrency 增加，仍受空间配额约束。凭据使用 `traits.envs` 的 Secret 引用。评测不接受自定义 Runner image/command、挂载、envFrom、ServiceAccount、任意 Python adapter 或环境 kwargs。

## 按需 Sandbox 与故障边界

新建评测由 Eruun 创建 `agents.kruise.io/v1alpha1` 的 `Sandbox`，Harbor 通过内部 API 申请并使用实际 Pod。ACK 内部署使用本集群 ServiceAccount 连接；dynamic client 用于 CRD，不提供运行中切换集群。无需 SandboxClaim/SandboxSet 预热池，也不会在 Sandbox API 失败时改建普通 Pod。云侧需安装匹配的 Sandbox controller、ACS agent-sandbox 算力，并通过 NetworkPolicy、exec、文件传输与真实配额预检；CRD 缺失不阻塞普通 API，但新 Sandbox 请求返回不可用。

每个 trial 保存 workspace/task/execution/trial、请求摘要、Runner UID、Sandbox UID、Pod UID。重复请求重用同一意图，变更镜像或规格返回冲突；同名替换不继承身份。Ready condition、observedGeneration 和真实 Pod 归属共同决定可连接状态；Sandbox Running 不等于评测成功。公开 Job 详情的 `sandboxes` 是生命周期投影，用户据此查看等待原因与保留时间，不能用投影代替执行或交付证明。 每次最多返回 100 条，优先仍占用资源的记录，其后按创建时间和 ID 倒序；`sandboxesTruncated=true` 表示还有未返回历史，此视图不是完整 trial 审计。数据库中的历史记录不会因响应截断而删除。

API/DB 连续中断期间，已有健康 trial 继续执行，新环境暂停申请；恢复后补报。运行中连续故障超过 600 秒则停止 Harbor 并进入有界收尾；收尾的 upload 与 terminal 仍可在同一 960 秒窗口内重试，确认失败结果不恢复计算。全局准入等待受 Job 总期限约束，不消耗 task 的环境启动预算；实际接纳后再计环境启动时间，API 暂时不可用时暂停该启动计时。agent/verifier 自身时限仍然生效。

任务结束后，未完整取回文件/日志的 Sandbox 保留 24 小时，记录 `retainUntil` 并同步云侧 `shutdownTime`，由 Eruun 维护循环和云侧自动清理共同约束。保留占用仍计容量；完整采集后请求 UID 条件删除，删除确认前不释放占用。Sandbox 不以 Runner 为 Kubernetes owner，避免 Runner 消失触发提前 GC。保留用于排查或人工补采，不代表第一阶段已经实现 Checkpoint 或计算恢复；结果归档的保存期限是另一套策略。

Runner 工作目录使用有大小上限的 emptyDir，`runnerWorkStorageMiB` 默认 20480，同时用于 ephemeral-storage request/limit；可在管理员部署配置中设为 1024..1048576 MiB。预算覆盖任务包、展开文件、Harbor 输出、日志及归档临时副本，应在长稳前按任务规模确定。磁盘耗尽或被 kubelet 驱逐会留下失败/交付未确认状态，不会把截断结果报告为完整。每个 trial 的磁盘声明由原生 task 的 `storage_mb` 给出；实际 ACS 规格、计费与 quota 需独立核验。

## Application / Workflow 中的评测

Application 的顶层组件可使用同一段 `{"name":"model-benchmark","type":"job","traits":{...}}`，无需填写 image。Workflow 使用既有 `jobType: deploy` 引用该组件，不新增评测步骤类型或调度器。可执行结构见 [应用内评测示例](../examples/agent-evaluation/application.json)。原生 task 内的步骤、工具调用和 verifier 仍由 Harbor 执行，不拆成 Eruun Workflow steps。

评测只适用于顶层 `job`；不能放在 webservice、initContainer 或 sidecar 中，也不能同时声明容器命令、端口、schedule、startTime、runPolicy 或 retry。Workflow 的执行顺序、并发与失败策略继续生效。取消应用内评测通过所属应用的 Workflow 取消接口；独立 `/jobs/:taskID/cancel` 不扩展为应用取消入口。

保留 Sandbox 为补采维持容器；Runner 丢失后，内部进程可能继续运行并消耗算力，直到显式清理或云侧 shutdownTime。第一阶段不提供进程冻结，也不把 Job 的执行期限等同于控制面故障下所有云资源已经物理终止。

## 原生任务包

上传格式为 `application/gzip` 的 tar.gz，支持根目录单个任务或多个任务目录。每个任务包含 `instruction.md`、`task.toml`、`environment/Dockerfile`、`tests/test.sh`，以及所需输入文件和可选 `solution/solve.sh`。用户定义 verifier 和评分规则，平台保留其奖励、日志与输出。

`task.toml` 的 `[environment].docker_image` 必须引用显式标签或 digest 的预构建镜像。Sandbox 路径还要求显式正整数 `storage_mb`（1..1048576 MiB）；不能依赖 Harbor 0.22.0 的空值作为默认磁盘。用户在本机构建并推送镜像，平台只拉取、执行，不构建 Dockerfile。任务包可重复用于不同 Job，不随某次结果到期删除。

```sh
tar -czf task-package.tar.gz -C examples/agent-evaluation harbor-task
curl -X POST "$ERUUN_URL/api/v1/job-datasets?name=harbor-demo" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "X-Eruun-Workspace-ID: $WORKSPACE_ID" \
  -H 'Content-Type: application/gzip' --data-binary @task-package.tar.gz
```

完整的构建、上传、提交、查询和结果下载流程见 [Harbor 评测 Job 示例](../examples/agent-evaluation/README.md)。先按 [示例镜像说明](../examples/agent-evaluation/harbor-task/README.md) 构建并调整镜像引用，再打包。Task 包压缩上限 64 MiB、展开上限 256 MiB；结果分别为 512 MiB、2 GiB；最多 10,000 条目。拒绝路径逃逸、重复歧义条目、任务包链接及特殊文件。超过边界会明确失败，不宣称已完整采集。

## 结果与保存

`GET /api/v1/jobs/:taskID` 分别返回队列执行 `status`、`executions`、`collectionState`、`results` 和 `deliveries`。评测 Job 在 Runner 已认领后还返回可选 `runnerStatus`：`phase`、已接受的 `sequence`、`lastHeartbeatAt`、`stale`、单调 trial 进度和不可变 terminal。该对象不包含任务 Token、Pod/Job UID 或执行密钥。心跳按 15 秒发送；服务端连续 60 秒未接收任何 Runner 事件时只将 `stale` 标为 true，不据此推断成功或失败，`lastHeartbeatAt` 仍只记录最后一次已接收心跳。`collectionState` 为 `pending`、`collected`、`incomplete`、`unavailable` 或 `expired`。原始结果的 `summary.executionStatus` 保留框架事实，`summary.collectionComplete` 表示采集完整性。

Application 内评测使用 `GET /api/v1/jobs/:taskID?executionKey=<Job executionKey>`，结果下载、保存重试和保留期接口同样传 `executionKey`。键来自 Workflow 的 Job 执行记录，不能用组件名替代。独立评测只有一个执行时可省略；存在多个执行时必须显式选择。结果和 delivery 都记录该键，避免同一父任务内互相覆盖。所选执行的有效声明与框架版本作为可查询快照保存，Runner 能力 Token 不返回给用户。

原始结果是完整 tar.gz：根 `result.json` 描述采集；`outputs/` 保留 Harbor 的结果、trial、奖励、轨迹、日志、产物及其他文件。文件清单和摘要便于查询，下载仍提供完整归档。采集完整性同时检查试验 Pod 的下载和 Runner 本地归档，Harbor 内部吞掉的下载异常也会标记为不完整。原生失败、取消、采集失败和无法上传分别可辨认；reward 为 0 本身不是运行失败。

源数据被事务性保存后，各目标记录为 `pending`。Controller 自动执行保存，状态独立为 `pending/running/succeeded/failed`，失败记录 `lastError`。用户仅需重试失败目标；成功目标重复重试为幂等操作，不重复评测。相同源归档重复上传被接受，不同内容不可覆盖已发布源。

- `minio/full` 保存完整文件；bucket 必须由管理员预先创建。
- `database/full` 在平台已配置的数据库保存元数据和文件字节，采用 1 MiB 分块。
- `database/metadata` 保存索引和文件引用，必须同时选择 `minio/full`；MinIO 成功后才能完成此目标。

一个目标失败不会回滚其他成功目标或改变已经完成的评测事实。失败保存需要显式重试；进程重启中断的保存由数据库租约恢复。原始数据过期后，未成功的保存无法再用该来源重试，返回 HTTP 410。

## 策略与生命周期

注册时初始化个人空间默认策略；团队空间创建时同样初始化。已有空间未存策略时使用同一默认值。通过现有空间成员授权使用管理员提供的数据库和 MinIO 能力，不另建用户身份系统。

默认策略为 `retentionDays: 90` 和 `database/full`。`GET /api/v1/job-storage-policy` 返回可用目标及当前策略；`PUT` 使用与 `resultPolicy` 相同的 JSON 更新默认值，范围 1–3650 天。独立提交时、应用内评测首次生成执行时，快照化 `traits.eval.resultPolicy`；省略则使用当时的空间默认值。恢复沿用已持久化的策略。修改默认策略只影响后续执行。

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

`/job-runners/:taskID/dataset`、`/job-runners/:taskID/results` 和 `POST /job-runners/:taskID/events` 是内部 Runner 接口：校验对应 Job 的能力凭据、Pod 名称/UID、所属 live Job UID、ExecutionKey、RunGeneration、Attempt 与持久化 checkpoint，登录 Token 不能代替 Runner 身份。Runner 必须先成功提交 `claim`，才能下载任务包或上传结果；同一 attempt 只有一个 Pod owner，不同 Pod 的认领返回 HTTP 409。旧执行者不能覆盖新执行结果。

父 Workflow 在租约回收后暂处于 Waiting/Queued 时，只有已经 claim、完整身份核验通过且原执行尚未过期的 Runner 收到可重试 HTTP 503，等待恢复 Running；此期间不接纳事件、结果或新 Sandbox 分配。事务锁内会再次验证当前执行身份和父状态，防止鉴权后发生接管竞态。已持久化但 ACK 丢失的 terminal 同样可等待恢复后幂等确认；无效身份、未 claim、过期或父任务终态不获得恢复豁免，执行 deadline 不延长。

事件请求上限 64 KiB，固定使用 `protocolVersion: "v1"`、正整数 `sequence` 与 `kind`。`kind` 为 `claim`、`phase`、`heartbeat`、`progress` 或 `terminal`；phase 只允许 `preparing/running/finalizing`；progress 只包含非负、单调且不超过总量的 `completedTrials/totalTrials`。terminal 使用 `succeeded/failed/cancelled/timed_out` outcome，引用 results 接口已确认的 64 字符 artifact ID 和 SHA-256 digest，并携带 `collectionComplete`；可选 exit code、signal、最长 64 字符稳定 reason 和最长 512 字符脱敏 message。

ACK 的 `data` 为 `{"acceptedSequence": 12, "action": "continue"}`，或在停止时额外返回 `"stopOutcome": "cancelled"|"timed_out"`。Runner 保存该权威原因并用它生成后续 terminal。相同 sequence 和内容重放是幂等操作，更旧 sequence 返回当前确认游标；同 sequence 不同内容、阶段/进度倒退、terminal 冲突或不同 owner 返回业务错误 `34004`（HTTP 409）。如果冲突仅由父任务已取消或到达绝对 deadline、而 terminal outcome 与权威状态不一致引起，409 的 `data.stopOutcome` 同样返回 `cancelled` 或 `timed_out`，Runner 使用相同 sequence 按该 outcome 重投；其他冲突不返回停止原因。服务端接收时间是心跳和状态陈旧计算的事实源。claim owner、最后事件摘要、sequence、phase、进度和 terminal 保存在当前 JobInfo `internal_info` 的 `runner` 子对象，不增加表或数据库列。

结果完整、`succeeded` terminal 获得 ACK、Runner 以 0 退出且 Kubernetes Job 成功，四项证据同时满足后控制面才能完成评测。状态 API 或数据库短暂故障只触发有界重试，不重启 Harbor；无法确认 terminal 时 Runner 非零退出，由 Kubernetes 证据收敛。

同一鉴权边界下增加 `POST /job-runners/:taskID/sandboxes`（`trialId/image/storageMiB`）、`GET .../sandboxes/:trialID` 和 `POST .../sandboxes/:trialID/release`（`sandboxUID/podUID/collectionComplete`）。响应 `data` 返回 pending/ready/retained/released/failed、`admitted`、实际资源身份及保留期限；尚未创建的容量/速率等待为 `admitted=false`。此为 Runner 私有 HTTP 协议，公共 HTTP/gRPC 只暴露授权后的生命周期投影。Token 与准入门控文件位于 Runner 私有工作目录，不进入任务包或结果归档。

## 管理员配置与部署

普通 `command` 不依赖 Harbor 配置。启用评测时，为所有运行角色设置 `--jobs-config-file` / `ERUUN_JOBS_CONFIG_FILE`，指向只读 Secret JSON：

```json
{
  "runnerImage": "registry.example.com/eruun-harbor-runner:0.22.0",
  "runnerWorkStorageMiB": 20480,
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

Helm 使用 `jobs.existingSecret` 和 `jobs.key` 挂载用户已创建的 Secret，Chart 不生成或公开存储凭据。四种角色需要相同配置；Controller 执行结果保存和 Sandbox 保留清理，Worker 执行 Harbor Job。API/Controller 获得指定 Sandbox CR 的独立 RBAC；Worker 不需 Sandbox 写权限。提供的单文件清单不默认启用 Harbor；使用 Helm 或为清单各角色手动挂载配置。

阶段一升级顺序：先运行 schema 迁移并部署新 RBAC；完成所有 API/Controller 读方升级，确认旧 Runner 的 v1 事件与 Pod 路径仍可用；再部署支持两种配置的 Runner 镜像和新 Worker，后者为新执行生成 `sandboxURL`；全部 Server 升级后才写新增 scheduler/部署配置字段。旧配置无 `sandboxURL` 时，新 Runner 仍读取旧 Pod 协议；已经运行的旧 Runner 沿原资源完成，不能临时把它的 Pod 当作 Sandbox。禁止新 Worker 配旧 Runner 镜像，或新 Sandbox 请求落到旧 API。使用明确的新镜像 tag/digest，避免覆盖同名镜像。回滚前停止新准入并排空 Sandbox 执行和保留资源，不能将新字段交给旧版严格解码器。

若来自更早的**无 claim 协议**版本，仍必须停止新评测并排空或明确取消旧评测、确认 Kubernetes Job 已停止，再升级并恢复提交；该版本不在上述已有 v1 Runner 的共存范围内。`command` 不进入此协议排空要求。

## 验证

Go 测试覆盖类型校验、空间授权、Runner 执行身份和恢复、完整归档、并发发布、独立保存及过期。存储集成测试使用真实 MySQL 8.4 和 MinIO；运行方式：

```sh
go test -race -tags=integration ./pkg/apiserver/jobs ./pkg/apiserver/jobs/artifacts
```

需要 `MYSQL_TEST_DSN` 和 `MINIO_TEST_CONFIG` 指向隔离环境；未配置时集成部分跳过，不能作为真实数据库验证。Runner claim 的 MySQL 测试验证行锁下只有一个 Pod 获胜。Python 测试和本地镜像构建见 [Runner README](../runners/harbor/README.md)。不同付费模型、私有镜像仓库、生产 CNI 网络规则，以及真实集群 OOM/网络恢复矩阵仍需部署方验证。
