# Harbor 评测 Job 示例

> 状态：Current。提供一个不依赖模型凭据的 Harbor `0.22.0` 端到端示例。示例使用 `oracle` 执行参考解，适合先验证任务镜像、原生任务包、`traits.evaluation` Job、结果采集和数据库保存链路；它不代表模型能力分数。

完整 API、权限和运行边界见 [空间 Job 与 Harbor 评测 API](../../docs/workspace-jobs-api.md)。

## 前置条件

- Eruun 已配置 Harbor Runner，API 可访问。
- Bearer Token 在目标空间至少具有 member 权限。
- 本机已安装 Docker、`tar` 和 `jq`。
- 有一个 Kubernetes 集群可拉取的镜像仓库。

设置请求参数和示例镜像地址：

```bash
export ERUUN_API_URL=http://127.0.0.1:8000
export ERUUN_TOKEN='<登录返回的 Bearer Token>'
export ERUUN_WORKSPACE_ID='<目标空间 ID>'
export HARBOR_TASK_IMAGE='example.com/your-team/eruun-greeting-task:1.0.0'
```

`HARBOR_TASK_IMAGE` 会直接传给 `docker build -t` 和 `docker push`，因此必须使用显式且非 `latest` 的标签，并替换为目标集群实际可拉取的地址。本例不演示如何在推送后将 `task.toml` 固定为镜像 digest。

## 1. 构建并推送任务环境镜像

```bash
docker build \
  -t "$HARBOR_TASK_IMAGE" \
  examples/agent-evaluation/harbor-task/environment
docker push "$HARBOR_TASK_IMAGE"
```

任务要求、参考解和 verifier 分别位于 `instruction.md`、`solution/solve.sh` 和 `tests/test.sh`。镜像与任务格式的说明见 [原生 Harbor 任务示例](harbor-task/README.md)。

## 2. 打包并上传原生任务

复制任务目录到临时位置并写入实际镜像地址，避免修改仓库中的占位示例：

```bash
HARBOR_EXAMPLE_DIR="$(mktemp -d)"
cp -R examples/agent-evaluation/harbor-task "$HARBOR_EXAMPLE_DIR/harbor-task"
sed -i.bak \
  "s|example.com/your-team/eruun-greeting-task:1.0.0|$HARBOR_TASK_IMAGE|" \
  "$HARBOR_EXAMPLE_DIR/harbor-task/task.toml"
rm "$HARBOR_EXAMPLE_DIR/harbor-task/task.toml.bak"
tar -C "$HARBOR_EXAMPLE_DIR" \
  -czf "$HARBOR_EXAMPLE_DIR/harbor-greeting.tar.gz" \
  harbor-task
```

上传 tar.gz，并从统一响应 envelope 中取得 dataset ID：

```bash
DATASET_RESPONSE="$(curl -sS -X POST \
  "$ERUUN_API_URL/api/v1/job-datasets?name=harbor-greeting" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -H 'Content-Type: application/gzip' \
  --data-binary @"$HARBOR_EXAMPLE_DIR/harbor-greeting.tar.gz")"

printf '%s\n' "$DATASET_RESPONSE" | jq .
DATASET_ID="$(printf '%s\n' "$DATASET_RESPONSE" | jq -er '.data.id')"
```

成功上传返回 HTTP 201。`DATASET_ID` 是平台生成的任务包 ID，可以被多个评测 Job 复用。

## 3. 提交评测 Job

使用 `evaluation.json` 生成包含当前空间和 dataset ID 的请求：

```bash
jq \
  --arg datasetId "$DATASET_ID" \
  '.traits.evaluation.taskPackageId = $datasetId' \
  examples/agent-evaluation/evaluation.json \
  > "$HARBOR_EXAMPLE_DIR/evaluation.json"

JOB_RESPONSE="$(curl -sS -X POST \
  "$ERUUN_API_URL/api/v1/jobs" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -H 'Content-Type: application/json' \
  --data-binary @"$HARBOR_EXAMPLE_DIR/evaluation.json")"

printf '%s\n' "$JOB_RESPONSE" | jq .
TASK_ID="$(printf '%s\n' "$JOB_RESPONSE" | jq -er '.data.taskId')"
```

成功提交返回 HTTP 202。请求使用 `database/full` 保存策略，因此不要求管理员额外配置 MinIO。

## 4. 查询执行与结果状态

重复查询下面的接口，直到 `status` 进入终态：

```bash
curl -sS \
  "$ERUUN_API_URL/api/v1/jobs/$TASK_ID" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  | jq '.data | {
      taskId,
      status,
      collectionState,
      runnerStatus,
      executions,
      results,
      deliveries
    }'
```

成功链路最终应满足：

- `status` 为 `completed`；
- `collectionState` 为 `collected`；
- `runnerStatus.terminal.outcome` 为 `succeeded`，且 `collectionComplete` 为 `true`；
- `deliveries` 中 `target=database` 的记录最终为 `state=succeeded`。

如果 Job 已完成但 delivery 仍为 `pending` 或 `running`，继续查询即可；结果保存与评测执行是两个独立阶段。

## 5. 下载并检查完整结果

读取结果列表并下载 Runner 上传的原始归档：

```bash
RESULTS_RESPONSE="$(curl -sS \
  "$ERUUN_API_URL/api/v1/jobs/$TASK_ID/results" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID")"

printf '%s\n' "$RESULTS_RESPONSE" | jq .
SOURCE_ARTIFACT_ID="$(printf '%s\n' "$RESULTS_RESPONSE" \
  | jq -er '.data.artifacts[] | select(.kind == "source") | .id')"

curl -sS \
  "$ERUUN_API_URL/api/v1/jobs/$TASK_ID/results/$SOURCE_ARTIFACT_ID/download" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -o "$HARBOR_EXAMPLE_DIR/results.tar.gz"

tar -tzf "$HARBOR_EXAMPLE_DIR/results.tar.gz"
tar -xOzf "$HARBOR_EXAMPLE_DIR/results.tar.gz" result.json | jq .
```

归档中的 `outputs/` 保留 Harbor 原生结果、trial 日志、verifier 的 `reward.txt` 和示例生成的 `greeting.txt` 制品。也可以在 database delivery 成功后下载其完整副本：

```bash
curl -sS \
  "$ERUUN_API_URL/api/v1/jobs/$TASK_ID/deliveries/database/download" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -o "$HARBOR_EXAMPLE_DIR/database-results.tar.gz"
```

## Application 内运行

`application.json` 将同一 `traits.evaluation` 放入顶层 Job 组件，由 Workflow 的 `deploy` 步骤执行。先替换任务包 ID，再按应用创建/执行 API 提交。模型评测时把 `agent` 改为受支持的 harness，填写并列的 `model` 字符串，并通过组件 `traits.envs` 引用凭据。应用内结果操作传入 `?executionKey=<该 Job 的执行键>`，取消通过所属应用 Workflow 接口完成。

## 示例文件

| 文件 | 用途 |
| --- | --- |
| `evaluation.json` | 使用 `oracle` 和上传后的 dataset 提交 Harbor 评测 Job。 |
| `application.json` | Application/Workflow 内复用相同评测声明。 |
| `command.json` | 同一空间 Job API 的普通命令对照示例。 |
| `harbor-task/` | 可打包的 Harbor 原生任务、环境镜像、参考解和 verifier。 |
