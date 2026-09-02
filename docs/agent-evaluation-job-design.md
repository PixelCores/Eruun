# Eruun Agent 评测 Job 设计


## 1. 文档关系

- [企业级分布式运行时设计](enterprise-distributed-runtime-design.md) 提供 Worker、数据库租约、fencing、Kubernetes 观察和角色化部署基础。
- [Workflow 全局调度层设计](workflow-global-scheduler-design.md) 负责 Agent 评测的优先级、公平性、项目配额和协作式抢占。
- 本文只定义一种新的系统任务和内部执行方式，不创建第二套顶层任务队列。
- 当前 Kubernetes 一次性 Job 控制器可作为实现参考，但其日志和状态能力不足以直接承载结构化评测结果。

## 2. 目标与非目标

v1 目标：

- 创建不依赖 Application 的独立 Agent 评测任务。
- 评测标准 OpenAI-compatible Chat Completions endpoint。
- 从 S3、OSS 或 HTTPS 读取 JSONL 数据集。
- 提供确定性评分、性能/用量指标，以及可选的 OpenAI-compatible LLM Judge。
- 使用 Eruun 内建、版本化 Eval Runner，以 Kubernetes Job 执行。
- 数据库保存查询摘要，对象存储保存逐 case 结果、trace、报告和 checkpoint。
- 默认以低优先级、可抢占方式进入统一 Workflow 调度队列。
- threshold miss 默认只产生独立 verdict；显式启用 `qualityGate` 时才使 Workflow 失败。

v1 不包含：

- 训练、微调、模型托管或在线 Agent 网关。
- 浏览器/桌面交互式 Agent benchmark。
- 跨集群分片、分布式 MapReduce 评测或第三方评测 SaaS 编排。
- exactly-once 模型调用；网络不确定性下个别 case 可能被重复请求。

## 3. 统一任务模型

### 3.1 Workflow 表示

Agent Evaluation 是系统内建的单步骤 Workflow Run：

| 字段 | 值 |
| --- | --- |
| `WorkflowQueue.taskId` | 新评测任务 ID |
| `WorkflowQueue.projectId` | 必填的项目隔离与公平调度键 |
| `WorkflowQueue.appId` | 空字符串，不创建 Application |
| `WorkflowQueue.workflowId` | 固定内建 ID `agent-evaluation-v1` |
| `WorkflowQueue.type` | 新增 `agent_evaluation` |
| `WorkflowQueue.status` | 从统一 `waiting -> queued -> running -> terminal` 状态机推进 |
| `WorkflowQueue.spec` | 版本化 `AgentEvaluationSpec` JSON |

新增 `WorkflowTaskTypeAgentEvaluation = "agent_evaluation"`，但不新增 `EvaluationQueue`。Domain Service 负责校验请求、保存 `WorkflowQueue` 并返回 `taskId`；全局 Scheduler 与其他 Workflow 使用相同路径调度它。

内建 Workflow 只有一个业务步骤：

```text
WorkflowRun(type=agent_evaluation)
  -> Step: agent-evaluation
      -> JobTask(executionKind=agent_eval)
          -> Kubernetes Job(Eval Runner)
```

### 3.2 类型边界

当前 `config.JobType` 同时承载组件类型（例如 `job`、`scheduledjob`）和执行动作（例如 `instant_job`）。实现评测能力时在 Go 内部拆分：

```go
type ComponentType string
type ExecutionKind string

const ExecutionKindAgentEval ExecutionKind = "agent_eval"
```

- `ComponentType` 只描述 Application component。
- `ExecutionKind` 只描述 WorkflowCtl 运行的内部动作。
- 已有 JSON/YAML/API 字符串和值保持兼容，不对现有客户端做破坏性改名。
- `agent_eval` 只允许由内建评测 Workflow 生成，不加入用户 Application component allowlist。

## 4. Public API

新增：

| Method | Path | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/agent-evaluations` | 创建评测 Workflow Run |
| `GET` | `/api/v1/agent-evaluations/:taskId` | 查询评测状态、进度、摘要和 verdict |
| `GET` | `/api/v1/agent-evaluations/:taskId/report` | 获取报告清单和授权后的短期制品链接 |

取消统一使用 `POST /api/v1/tasks/:taskId/cancel`；通用任务详情也可从 `GET /api/v1/tasks/:taskId` 查询。所有接口沿用 Eruun 统一成功/错误 envelope，并以 `projectId` 做授权。

### 4.1 创建请求

```json
{
  "projectId": "project-a",
  "idempotencyKey": "nightly-agent-a-2026-08-27",
  "priority": "low",
  "deadlineAt": null,
  "target": {
    "baseURL": "https://agent.example.com/v1",
    "model": "agent-model-a",
    "apiKeySecretRef": {
      "name": "agent-target-credentials",
      "key": "api-key"
    },
    "headers": {
      "X-Tenant": "evaluation"
    }
  },
  "dataset": {
    "uri": "s3://eval-datasets/support/v3.jsonl",
    "sha256": "optional-lowercase-hex"
  },
  "scorers": [
    {"type": "exact_match"},
    {"type": "regex_match"},
    {"type": "json_schema"}
  ],
  "judge": {
    "enabled": true,
    "baseURL": "https://judge.example.com/v1",
    "model": "judge-model",
    "apiKeySecretRef": {
      "name": "judge-credentials",
      "key": "api-key"
    },
    "rubric": "Score correctness from 0 to 1. Return JSON only."
  },
  "quality": {
    "thresholds": {
      "successRate": 0.98,
      "scores.exact_match": 0.95,
      "scores.llm_judge": 0.8,
      "p95LatencyMs": 5000
    },
    "qualityGate": false
  },
  "execution": {
    "maxConcurrency": 8,
    "requestsPerSecond": 0,
    "requestTimeoutSeconds": 60
  },
  "runtime": {
    "resources": {
      "requests": {"cpu": "500m", "memory": "512Mi"},
      "limits": {"cpu": "2", "memory": "2Gi"}
    },
    "nodeSelector": {},
    "tolerations": []
  },
  "preemptible": true
}
```

默认值：

- `priority=low`。
- `preemptible=true`。
- `execution.maxConcurrency=8`。
- `execution.requestsPerSecond=0`，表示不额外限速；仍受并发限制。
- `execution.requestTimeoutSeconds=60`。
- `judge.enabled=false`。
- `quality.qualityGate=false`。

`quality.thresholds` v1 只接受以下键，所有阈值同时满足时 verdict 才是 `passed`：

- `successRate`：取值范围 `[0,1]`，实际值必须 `>=` 阈值。
- `scores.<scorer-name>`：取值范围 `[0,1]`，scorer 名必须来自本次请求配置的 scorer（`llm_judge` 还要求 `judge.enabled=true`），实际值必须 `>=` 阈值。
- `p95LatencyMs`：必须为大于 0 的毫秒数，实际 p95 latency 必须 `<=` 阈值。

未知键、拼写错误、越界值或引用未启用 scorer 的阈值都在请求校验阶段 fail-fast，不得静默忽略或套用默认比较方向。

`idempotencyKey` 按当前 WorkflowQueue 唯一约束做全局去重：同一 key、同一 project 且 spec 等价时返回已有 `taskId`；跨 project 复用或 spec 不同都返回冲突，并且不能向无权调用方泄露已有任务信息。等价性由“应用默认值、按字段名规范化 JSON、排除 idempotencyKey 后”的 SHA-256 `specHash` 判断。API 不接受明文 API key、Bearer token 或对象存储密钥。

`target.headers` 只允许策略配置的非敏感 header；拒绝 `Authorization`、`Cookie`、`Proxy-Authorization` 和其他凭据类 header。认证只能通过 SecretRef 注入。

### 4.2 查询响应

评测投影至少包含：

```json
{
  "taskId": "task-123",
  "projectId": "project-a",
  "status": "completed",
  "verdict": "failed",
  "qualityGate": false,
  "progress": {
    "total": 1000,
    "completed": 1000,
    "succeeded": 972,
    "failed": 28
  },
  "summary": {
    "successRate": 0.972,
    "latencyMs": {"p50": 820, "p95": 4300, "p99": 6200},
    "tokens": {"input": 1200000, "output": 340000},
    "scores": {"exact_match": 0.94, "llm_judge": 0.86}
  },
  "artifactStatus": "ready",
  "checkpointAt": "2026-08-27T10:12:30Z",
  "createdAt": "2026-08-27T10:00:00Z",
  "completedAt": "2026-08-27T10:20:00Z"
}
```

`report` API 返回 manifest 和经当前调用者授权后生成的短期下载链接，不直接代理大型结果文件，也不返回对象存储永久凭据。

## 5. Dataset 契约

支持 URI scheme：

- `s3://bucket/key`：S3-compatible 数据源。
- `oss://bucket/key`：Aliyun OSS 数据源。
- `https://host/path`：受 URL Security Policy 和 allowlist 约束的 HTTPS 数据源。

数据必须是 UTF-8 JSONL。每行：

```json
{
  "id": "case-0001",
  "messages": [
    {"role": "system", "content": "You are a support agent."},
    {"role": "user", "content": "How do I reset my password?"}
  ],
  "expected": {
    "text": "Open Settings and choose Reset Password.",
    "regex": "(?i)reset password",
    "jsonSchema": {
      "type": "object",
      "required": ["steps"]
    }
  },
  "metadata": {
    "category": "account"
  }
}
```

约束：

- `id` 在一个数据集中必须唯一且非空，是恢复和去重键。
- `messages` 必须符合 OpenAI-compatible role/content 结构。
- `expected` 可选；启用相应 scorer 时所需字段缺失，该 case 记为 scorer error，不静默跳过。
- `metadata` 只参与过滤、分组和报告，不注入模型请求。
- Runner 启动时记录 dataset size、ETag（若有）和 SHA-256。请求提供 `sha256` 时不匹配立即失败。
- checkpoint 恢复时 dataset hash 必须与首次执行完全一致。

## 6. 评分与指标

### 6.1 自动指标

每个评测自动统计：

- 请求成功率、HTTP/协议/超时错误分类。
- 端到端 latency 的 p50、p95、p99。
- input/output/total token；目标未返回 usage 时标记 `unavailable`，不伪造估算值。
- case 完成数、失败数、重试数和可能重复请求数。

### 6.2 确定性 scorer

v1 内建：

- `exact_match`：规范化换行和首尾空白后，与 `expected.text` 精确比较。
- `regex_match`：使用 `expected.regex` 匹配完整模型文本；非法表达式是数据错误。
- `json_schema`：解析模型文本为 JSON，并使用 `expected.jsonSchema` 校验。

每个 scorer 输出 case 级 `passed|failed|error` 和聚合通过率。Runner 版本固定规范化规则，升级规则需要增加 spec/runner 版本，不能静默改变历史结果。

### 6.3 LLM Judge

Judge 使用独立的 OpenAI-compatible endpoint、model 和 SecretRef，不复用目标 Agent 凭据。输入包含 rubric、原始 messages、Agent 输出及可选 expected；要求 Judge 返回：

```json
{"score": 0.86, "reason": "..."}
```

`score` 必须在 `[0,1]`。无效 JSON、越界分数、超时和 Judge 服务错误记为 judge error；默认不把它当作目标 Agent 请求失败，但如果质量阈值依赖 `scores.llm_judge`，缺失分数会使该阈值不通过。Judge reason 属于详细制品，不写入常规日志。

## 7. Eval Runner

### 7.1 版本和 Pod

Eruun 发布一个内建、不可变 tag 或 digest 固定的 Eval Runner image。`AgentEvaluationSpec` 保存 `runnerVersion`；若请求未指定，由 API 固化当前服务端默认值，重试和恢复不能自动漂移到新 Runner。

Worker 为评测生成 Kubernetes Job：

- 固定 namespace：`eruun-evaluations`。
- 名称包含 taskId 的安全摘要和 run generation，避免冲突。
- labels/annotations 包含 managed-by、taskId、projectId 摘要、generation 和 execution kind。
- `backoffLimit: 0`，业务重试由 Eruun generation/attempt 管理，避免 Kubernetes 与 Eruun 双重重试。
- `restartPolicy: Never`，`terminationGracePeriodSeconds` 不小于 30。
- 挂载目标/Judge Secret 的指定 key，不挂载整个 Secret。

Runner 启动流程：

1. 校验 spec version、runner version、task-scoped token 和输入 URI。
2. 下载并验证 dataset，计算稳定 spec hash 和 dataset hash。
3. 加载并验证 checkpoint；不存在则从头开始。
4. 按 `maxConcurrency` 与 requests-per-second 限制执行 case。
5. 按 caseExecutionId 保存结果，周期聚合摘要并发送 heartbeat/event。
6. 上传最终 manifest、逐 case 结果、trace 和报告，最后上报 completed/failed。

case execution id：

```text
sha256(taskId + specHash + datasetHash + caseId)
```

目标 endpoint 支持时以 `Idempotency-Key` 发送该值。若连接在响应前断开，Runner可以重试，但必须在报告中增加 `possiblyDuplicatedRequests`；Eruun 不宣称外部模型调用 exactly-once。

### 7.2 内部控制接口

Runner 只通过集群内部 Service 访问：

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/internal/v1/evaluation-runs/:taskId/control` | 长轮询 continue/checkpoint/stop/cancel 指令 |
| `POST` | `/api/internal/v1/evaluation-runs/:taskId/events` | 上报 heartbeat、progress、checkpoint、completed 或 failed 事件 |

每个事件携带 generation、event sequence 和 task-scoped token。服务端只接受当前 generation/token，event sequence 单调去重；内部接口不通过外部 Ingress 暴露。Runner token 由 Eruun 创建为专用 Secret，只允许访问一个 taskId 和 generation，终态后吊销。

## 8. 持久化和制品

### 8.1 数据库摘要

`JobInfo` 继续作为评测执行记录，并 additive 增加或在版本化 JSON 中保存：

- `executionKey`、`runGeneration`、`attempt`。
- `resultSummary`：进度、成功率、延迟、token、聚合 score 和 verdict。
- `artifactURI`、`checkpointURI`、`checkpointAt`。
- `controlState` 与最后处理的 event sequence。

数据库不保存完整 dataset、逐 case prompt/response、Judge reason、模型 API key 或对象存储凭据。

### 8.2 ArtifactStore

v1 使用 S3-compatible ArtifactStore；Aliyun OSS 可通过兼容 endpoint 或专用 adapter 接入同一接口。对象前缀：

```text
<prefix>/<projectId>/<taskId>/generation-<n>/
  manifest.json
  summary.json
  cases.jsonl.gz
  traces.jsonl.gz
  report.html
  checkpoint.json
```

Eruun 的对象存储主凭据只存在于 Kubernetes Secret。Runner 获取限定 bucket/prefix、方法和有效期的 presigned URL；URL 有效期覆盖任务 timeout 并预留 15 分钟，长任务在剩余 5 分钟时通过内部控制接口刷新。日志和 API 响应不得记录完整 presigned query。

上传完成后 Runner 先写不可变对象，再原子发布 `manifest.json`。只有 manifest 包含每个对象的 key、size、sha256 后，数据库 `artifactStatus` 才能变为 `ready`。

## 9. 状态、Verdict 与失败语义

`verdict` 取值：

- `passed`：所有配置阈值满足。
- `failed`：至少一个阈值未满足或依赖指标缺失。
- `not_evaluated`：没有配置 threshold。

Workflow 终态规则：

| 场景 | Workflow 状态 | Verdict |
| --- | --- | --- |
| 评测成功，无 threshold | `completed` | `not_evaluated` |
| 评测成功，阈值满足 | `completed` | `passed` |
| 阈值不满足，`qualityGate=false` | `completed` | `failed` |
| 阈值不满足，`qualityGate=true` | `failed` | `failed` |
| dataset、目标调用、Runner、Judge 必需指标或制品发布发生基础设施错误 | `failed` | 已有结果可计算则保留，否则 `not_evaluated` |
| 用户取消 | `cancelled` | 保留最后摘要，不生成通过结论 |
| deadline 到期 | `timeout` | 保留最后摘要，不生成通过结论 |

Judge 只有在 `judge.enabled=true` 时执行。Judge 全部不可用但没有任何阈值依赖 Judge 时，评测可以完成并在摘要中标记 judge error；依赖 Judge 的阈值无法计算时 verdict 必须失败。

## 10. Checkpoint、恢复与抢占

Runner 每 30 秒或每完成 100 个 case（先发生者）上传 checkpoint。checkpoint 至少包含：

```json
{
  "version": 1,
  "taskId": "task-123",
  "runnerVersion": "v1",
  "specHash": "...",
  "datasetHash": "...",
  "completedCaseIds": ["case-0001"],
  "aggregateState": {},
  "artifactParts": [],
  "createdAt": "2026-08-27T10:12:30Z"
}
```

服务端只有在对象存在、checksum 正确且 taskId/specHash/datasetHash/runnerVersion 匹配时才把 checkpoint 标记为 valid。恢复时忽略当前 run generation，但必须创建新的 JobInfo attempt；已完成 case 根据 caseExecutionId 跳过，未完成 case 继续执行。

协作式抢占：

1. Scheduler 把任务标为 `preempting`，通过 control 接口发送 `checkpoint`，Runner 保持运行。
2. 30 秒端到端预算从 control 请求发出时开始计时。Runner 立即冻结新的 case 领取，最多用前 20 秒等待在途请求；到达 drain deadline 后取消尚未完成的请求，这些 case 不写入 completed cursor，恢复后按 caseExecutionId 重试。
3. Runner 在第 20–25 秒内上传只包含已完成 case 的 checkpoint，并上报对象 checksum 和事件 sequence。
4. 服务端为第 25–30 秒预留校验时间；验证 checkpoint 后返回 `stop`，Runner 正常退出，Worker 清理本代 Job，任务恢复为 `waiting`。
5. 30 秒内没有完成上传、上报和服务端校验时，Scheduler 撤销请求并返回 `continue`；Runner 恢复 case 领取，原 Kubernetes Job 不被删除。
6. 用户 cancel 高于 preemption；收到 `cancel` 时不要求可恢复 checkpoint，但尽力上传最后摘要。

抢占不消耗失败 retry count，成功抢占计入 `preemptionCount`。只有本文件定义的纯评测 Workflow 可以进入该协议。

## 11. 安全边界

- `eruun-evaluations` 使用独立 ServiceAccount、ResourceQuota、LimitRange、Pod Security 和 NetworkPolicy。
- Runner ServiceAccount 默认没有 Kubernetes API 资源读写权限；只获得投影的 task token 和明确 Secret key。
- SecretRef 只能引用 `eruun-evaluations` namespace 中经过授权的 Secret，不能跨 namespace 任意读取。
- Target/Judge/Dataset URL 统一经过现有 URL Security Policy，阻止 loopback、link-local、metadata endpoint、私有网段绕过和重定向逃逸；企业管理员显式配置允许的域名/CIDR。
- NetworkPolicy 只放行 DNS、Eruun 内部控制 Service、对象存储、Target 和 Judge 允许目标。
- dataset、prompt、response、Judge reason 和报告按敏感数据处理：对象存储启用传输/静态加密、最小权限和生命周期清理，API 下载链接需要项目授权并短期有效。
- 常规日志只记录 taskId、generation、case 计数和错误分类，不记录消息内容、Secret、Authorization header 或 presigned URL。

## 12. 可观测性、测试与上线

最低指标：

- `eruun_evaluation_runs{status}`
- `eruun_evaluation_cases_total{result}`
- `eruun_evaluation_request_latency_seconds`
- `eruun_evaluation_target_errors_total{class}`
- `eruun_evaluation_judge_errors_total{class}`
- `eruun_evaluation_checkpoint_age_seconds`
- `eruun_evaluation_artifact_upload_bytes_total`
- `eruun_evaluation_preemption_total{result}`

测试：

- API/DTO/assembler/domain：创建、幂等冲突、project 授权、Secret/header/URL 校验和脱敏。
- Workflow：`agent_evaluation` 只生成一个 `agent_eval` Job，AppID 为空，不进入 Application callback 路径。
- Runner：JSONL 校验、并发/限速、exact/regex/JSON Schema、token 缺失、Judge 无效响应。
- 结果：DB 摘要、S3-compatible MinIO 制品、checksum、manifest 原子发布和授权下载。
- 恢复：Worker 崩溃、重复消息、旧 generation 事件、caseExecutionId 去重和 checkpoint 不匹配。
- 抢占：30 秒/100 case checkpoint、有效 checkpoint 后退出、无 checkpoint 时继续原任务、最多 3 次与冷却时间。
- 质量：无阈值、阈值通过、gate 关闭失败 verdict、gate 开启 Workflow failed、Judge 依赖缺失。
- 安全：Secret 不进入 DB/日志/API，内部 token 越权和重放被拒绝，恶意 URL/重定向被阻止。

上线顺序：先启用 ArtifactStore、namespace/RBAC/NetworkPolicy 和 Runner；再上线创建/查询 API，但以 Scheduler shadow 模式观察；最后启用统一调度和协作式抢占。任何阶段回滚都保留 WorkflowQueue、JobInfo 和制品记录，不删除用户已有评测结果。
