# Harbor 合成负载示例

> 状态：Current。提供可构建的无模型任务环境、原生 Harbor `oracle` 任务包及批量提交器；大规模集群容量仍须按 [压测计划](../../../docs/harbor-job-load-test-plan.md) 实测。

`task/environment/simulate.py` 在试验 Pod 中同时启动五个线程。每个线程的休眠时长不同，最长线程由 Pod hostname 的 SHA-256 值确定为 60–300 秒；同一个 Pod 的结果可复现，不需要模型、模型 Secret 或付费 API。`solution/solve.sh` 是 Harbor `oracle` 的参考解，生成 `/app/load-timing.json` 和原始制品 `/logs/artifacts/load-timing.json`；verifier 检查所有线程结束后给出 reward 1。休眠几乎不消耗 CPU，主要给提交、调度、Pod 生命周期、Runner 状态与结果保存制造持续负载；它不能代表真实模型的 CPU、内存、网络或 GPU 压力。

先在本机运行一个两秒的模拟器烟测（此命令不运行 Harbor）：

```sh
python3 examples/agent-evaluation/load-test/task/environment/simulate.py \
  /tmp/eruun-load-timing.json --min-seconds 1 --max-seconds 2
```

使用目标集群可拉取的显式镜像标签构建和推送环境镜像，然后在临时副本中替换 `task.toml` 的占位 `docker_image`，再打包并上传。Dockerfile 只在本机用于构建；Eruun 不负责构建用户镜像。

先设置隔离环境的 API 地址、空间 ID 和有 member 权限的 Bearer Token：

```sh
export ERUUN_API_URL='http://127.0.0.1:8000'
export ERUUN_WORKSPACE_ID='<目标空间 ID>'
export ERUUN_TOKEN='<登录返回的 Bearer Token>'
```

```sh
export HARBOR_LOAD_IMAGE='registry.example.com/team/eruun-harbor-load:1.0.0'
docker build -t "$HARBOR_LOAD_IMAGE" examples/agent-evaluation/load-test/task/environment
docker push "$HARBOR_LOAD_IMAGE"

LOAD_DIR="$(mktemp -d)"
cp -R examples/agent-evaluation/load-test/task "$LOAD_DIR/task"
sed -i.bak \
  "s|example.com/your-team/eruun-harbor-load:1.0.0|$HARBOR_LOAD_IMAGE|" \
  "$LOAD_DIR/task/task.toml"
rm "$LOAD_DIR/task/task.toml.bak"
tar -C "$LOAD_DIR" -czf "$LOAD_DIR/harbor-load.tar.gz" task

DATASET_RESPONSE="$(curl -sS -X POST \
  "$ERUUN_API_URL/api/v1/job-datasets?name=harbor-load" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  -H 'Content-Type: application/gzip' \
  --data-binary @"$LOAD_DIR/harbor-load.tar.gz")"
DATASET_ID="$(printf '%s\n' "$DATASET_RESPONSE" | jq -er '.data.id')"
```

`ERUUN_API_URL`、`ERUUN_TOKEN`、`ERUUN_WORKSPACE_ID` 及 `DATASET_ID` 必须指向隔离环境。先用 `--count 1` 做真实 Harbor 预检，确认 reward 1、`collectionState=collected`、数据库保存成功和 `/logs/artifacts/load-timing.json` 中的 `plannedJobSeconds`；最长为 300 秒时，连同 Harbor 准备和结果归档，一个 Job 可能超过五分钟。预检成功后再提交 1000 个：

```sh
python3 examples/agent-evaluation/load-test/submit.py \
  --api-url "$ERUUN_API_URL" \
  --workspace-id "$ERUUN_WORKSPACE_ID" \
  --dataset-id "$DATASET_ID" \
  --count 1 --rate 1 --output "$LOAD_DIR/preflight.jsonl"

TASK_ID="$(jq -er '.taskId' "$LOAD_DIR/preflight.jsonl")"
curl -sS "$ERUUN_API_URL/api/v1/jobs/$TASK_ID" \
  -H "Authorization: Bearer $ERUUN_TOKEN" \
  -H "X-Eruun-Workspace-ID: $ERUUN_WORKSPACE_ID" \
  | jq '.data | {status, runnerStatus, collectionState, deliveries}'
```

重复查询直到任务终态，检查完整采集、目标保存与原生结果中的 reward、`load-timing.json`。预检通过后再运行批量提交：

```sh
python3 examples/agent-evaluation/load-test/submit.py \
  --api-url "$ERUUN_API_URL" \
  --workspace-id "$ERUUN_WORKSPACE_ID" \
  --dataset-id "$DATASET_ID" \
  --count 1000 --rate 20 --workers 32 \
  --output "$LOAD_DIR/submit-1000.jsonl"
```

提交器读取环境变量 `ERUUN_TOKEN`，不把 Token 写入输出；每次运行生成唯一 `runId` 和 Job 名称。JSONL 含每条请求的计划/实际开始时间、完成时间、HTTP 状态、业务码、task ID 和接受判定，文件以 `0600` 创建；已有输出文件不会被覆盖。网络超时或 202 响应内容不完整时，脚本标为不确定并退出非零，**不会自动重试创建**。1000 个 task ID 是后续状态、Kubernetes 和结果指标的关联键；仅有 `accepted=1000/1000` 不能说明任务已运行或结果已保存。

请求模板是同目录的 `evaluation.json`，使用 `type=eval`，由服务端默认选择 Harbor 0.22.0，并选择 `oracle`、单 trial、900 秒 Job 执行预算及隔离数据库上的 `database/full` 保存目标。请求体省略 `workspaceId`；批量提交器仍用 `X-Eruun-Workspace-ID` 指定已授权空间，1000 个 Job 复用上传任务包的 `datasetId`。默认资源 Trait 为 Runner 和任务环境分别请求 1 CPU/2 GiB；并发增大前请按压测计划确认空间配额和节点容量。`--template` 可指定经过人工核对的其他请求模板，以便固定不同的资源或保存策略。压测结束后保留 JSONL、任务状态和监控记录，并按隔离环境流程清理镜像、数据库完整副本与 Kubernetes 资源；源归档的一天保留期不会自动删除数据库保存副本。
