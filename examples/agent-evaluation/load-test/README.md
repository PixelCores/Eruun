# Harbor 合成负载示例

> 状态：Current。提供可构建的无模型任务环境、原生 Harbor `oracle` 任务包、批量提交器与结果观察器；大规模集群容量仍须按 [压测计划](../../../docs/harbor-job-load-test-plan.md) 实测。

`task/environment/simulate.py` 在试验 Pod 中同时启动五个线程。每个线程的休眠时长不同，最长线程由 Pod hostname 的 SHA-256 值确定为 60–300 秒；同一个 Pod 的结果可复现，不需要模型、模型 Secret 或付费 API。`solution/solve.sh` 是 Harbor `oracle` 的参考解，生成 `/app/load-timing.json` 和原始制品 `/logs/artifacts/load-timing.json`；verifier 检查所有线程结束后给出 reward 1。休眠几乎不消耗 CPU，主要给提交、调度、Pod 生命周期、Runner 状态与结果保存制造持续负载；它不能代表真实模型的 CPU、内存、网络或 GPU 压力。

先在本机运行一个两秒的模拟器烟测（此命令不运行 Harbor）：

```sh
python3 examples/agent-evaluation/load-test/task/environment/simulate.py \
  /tmp/eruun-load-timing.json --min-seconds 1 --max-seconds 2
```

这里的“任务包”是上传到 `/api/v1/job-datasets` 的 tar.gz：包含 `instruction.md`（任务说明）、`task.toml`（镜像和超时）、`environment/Dockerfile`（本机构建镜像用）、`solution/solve.sh`（oracle 执行的参考脚本）和 `tests/test.sh`（验证完成）。Harbor 的 `oracle` 模式不会向模型提问，而是运行 `solve.sh`；该脚本调用镜像里的 `simulate.py`，五个线程休眠后生成时长文件，verifier 检查文件并给出 reward。上传时 Eruun 自动生成任务包 ID，响应的 `data.id` 就是提交 JSON 中的 `taskPackageId`；这个字段是 Eruun 的上传归档 ID，Harbor 实际接收的是 Runner 解包后的本地任务路径。

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

提交器结束后，用同一空间的 Token 观察已接受任务：

```sh
python3 examples/agent-evaluation/load-test/observe.py \
  --api-url "$ERUUN_API_URL" \
  --workspace-id "$ERUUN_WORKSPACE_ID" \
  --input "$LOAD_DIR/submit-1000.jsonl" \
  --output "$LOAD_DIR/observe-1000.jsonl" \
  --rate 10 --workers 8 --interval 30 --timeout 15 --deadline 1800
```

观察器只读取既有 `GET /api/v1/jobs/:taskID`。输入必须是已结束的提交 JSONL；接受记录须含有效 `taskId`、HTTP 202、业务码 0 和 `completedAt`。重复 ID 只观察一次，提交未接受记录单独计数；格式异常会在发请求前退出。它不重新提交、取消 Job、重试保存或下载结果。

- 全局 `--rate` 限制本进程全部 GET 的启动速率，不按 Job 各自限速，也不积攒突发额度。`--workers` 同时限制排队和在途请求总数。多个观察器进程的流量需要相加。
- `--interval` 是同一 ID 完成一次请求后，再发请求的最小间隔；实际间隔还受总任务数、全局速率、响应延迟和并发限制。例如 1000 个 ID、10 GET/s 时，仅覆盖一遍就至少需要约 100 秒，不能声称每个 ID 都以 30 秒采样。
- `--timeout` 是单次请求的 socket 超时，且不大于剩余总预算；`--deadline` 是整个观察阶段的时长上限。DNS、缓慢流式响应等超出 socket 超时覆盖的阻塞不会阻止命令在总 deadline 结束，剩余任务标为 `unknown/deadline`。结束时在途响应不作为成功证据。
- HTTP/API 错误、超时及无法识别的响应会记录固定错误类别，并在相同全局预算下重试 GET，直到收敛或总 deadline。最多读取 8 MiB 的单个详情响应；超过上限记录 `response_too_large`。重定向被拒绝，Token 不会随重定向转发。

成功要求：执行 `status` 为 `completed/passed`，`collectionState=collected`，且从 `job.traits.eval.resultPolicy.targets` 取得的**全部所选目标**在 `deliveries` 中均为 `succeeded`，保存模式与快照一致。目标缺失不会误报成功；不能从 `deliveries=[]` 推断“不需要保存”。`failed/timeout/cancelled/reject/notRun/skipped` 作为非成功执行立即记录，避免失败任务永久等待不存在的完整采集。成功执行如果采集 `incomplete/unavailable/expired` 或任一保存目标 `failed`，分别记录采集或保存失败；需要按已有 API 人工处理，不会被脚本改成执行成功。

输出文件以 `0600` 独占创建，拒绝覆盖。JSONL 依次包含输入统计、观察配置、状态变化/API 错误、每个 ID 的最终记录与汇总；状态未变时不重复写入，成功采样次数仍累计。记录仅保留 ID、固定状态/错误类别、计数和客户端时刻，不保存 Token、原始结果、manifest、执行详情、保存地址或错误正文。退出码 0 表示全部唯一已接受任务收敛成功且无未接受提交；存在失败、未知或未接受提交时为 1，输入/参数/输出文件错误为 2。

`observedTaskIds/uniqueTaskIds` 表示至少一次成功读取的覆盖范围；`successfulSamples`、每 ID 的 `samples`、`maxSampleGapSeconds`、`lastSampleAgeSeconds` 和 API 错误共同说明采样质量。`terminalObservedAt`、`collectedObservedAt`、`deliveredObservedAt` 分开记录首次观察时刻，三者可能顺序不同。时刻使用客户端 Unix 秒；间隔与 deadline 使用单调时钟。这些是被采样发现的时间，包含开始观察前和请求间隔内的盲区，不能当作服务端精确阶段延迟。观察从批量提交结束后开始，不覆盖提交时的全部瞬态变化。

`status=running` 与 `runnerPhase=running` 都只是 API 投影，采样并非所有任务同一时刻的快照。此工具不验证 Kubernetes Runner/trial 关联、trial 实际运行、reward 或原始制品内容，也不提供万级同时执行证明；真实执行数和容量结论须结合受控 Kubernetes/Harbor 证据及压测计划。预检中的 reward 和时长文件仍需单独核对。

本地测试不访问集群：

```sh
python3 -m unittest discover -s examples/agent-evaluation/load-test -p 'test_observe.py' -v
```

## 固定单 Worker 的手动容量阶梯

[`worker-baseline-values.yaml`](worker-baseline-values.yaml) 固定一个 Worker，request/limit 都为 4 CPU/8 GiB，初始 `ERUUN_WORKFLOW_MAX_CONCURRENT=100`。这是待实测的固定实验点，不是生产推荐或已验证容量；也可以在首轮前选定其他资源，但整组阶梯必须保持同一配置。API、Controller、Scheduler 保持环境 values/Chart 的 HA 副本设置。此参数仅限制每 Worker 并行 Workflow controller 数，不代表试验 Pod 数或已运行 trial 数。

开始前，按[部署契约](../../../docs/helm-deployment.md)准备受控环境 values、现有账号/Runner 配置 Secret 及可拉取的明确版本镜像。Chart 仍包含内置 MySQL/Redis；本覆盖文件不创建 ACS 算力、不配置云凭据，也不替环境完成 HA 数据面。**Helm 会替换 `env` 数组**；如果现有环境 values 中有 `env`（例如外部数据库连接配置），先将全部条目合并到覆盖文件的私有副本，确认 Worker slot 所在下标后再使用命令，不能用下方的 `env[0]` 覆盖其他设置。

下面命令由操作者在隔离集群手动执行；先设置真实的 `KUBE_CONTEXT`、`ERUUN_RELEASE`、`ERUUN_NAMESPACE`、`ERUUN_ENV_VALUES`、`ERUUN_IMAGE_REPOSITORY`、`ERUUN_IMAGE_TAG`。环境 values 含现有 auth/jobs Secret 名称及私有部署设置，权限应为 `0600`，不要提交。`BASELINE_VALUES` 指向本目录文件或已合并 env 的私有副本，`WORKER_SLOT_INDEX` 指向该副本中对应 env 项。

```sh
BASELINE_VALUES=examples/agent-evaluation/load-test/worker-baseline-values.yaml
WORKER_SLOT_INDEX=0
WORKER_SLOTS=100

# 仅本地渲染，不调用 Kubernetes；只显示运行角色清单。
helm template "$ERUUN_RELEASE" deploy/helm/eruun \
  --namespace "$ERUUN_NAMESPACE" \
  --values "$ERUUN_ENV_VALUES" --values "$BASELINE_VALUES" \
  --set-string image.repository="$ERUUN_IMAGE_REPOSITORY" \
  --set-string image.tag="$ERUUN_IMAGE_TAG" \
  --set-string "env[$WORKER_SLOT_INDEX].value=$WORKER_SLOTS" \
  --show-only templates/runtime-deployments.yaml

# 完成人工核对、已有 release 的任务排空后，再执行升级。
helm upgrade "$ERUUN_RELEASE" deploy/helm/eruun \
  --kube-context "$KUBE_CONTEXT" --namespace "$ERUUN_NAMESPACE" \
  --values "$ERUUN_ENV_VALUES" --values "$BASELINE_VALUES" \
  --set-string image.repository="$ERUUN_IMAGE_REPOSITORY" \
  --set-string image.tag="$ERUUN_IMAGE_TAG" \
  --set-string "env[$WORKER_SLOT_INDEX].value=$WORKER_SLOTS" \
  --wait --timeout 10m
```

渲染可能包含私有 env 值，仅在受控终端审阅，不把输出贴到公共记录。每档完整排空、核对结果并保存观测后，手动把 `WORKER_SLOTS` 改为 **100 → 250 → 500 → 1000**，再次渲染/升级。每档只变该 slot，固定 Worker 资源、副本、任务模板、请求速率、其余角色配置与观察预算；禁止自动循环放量。Deployment 滚动升级期间可能临时存在新旧 Worker 重叠，须等旧 Pod 退出、确认只有一个有效 Worker，再开始该档注入。

准入前置：固定并记录 [`workflow_scheduler`](../../../docs/system-setting.md#job-全局调度) 的全局 `maxConcurrentJobs`、各空间 `maxConcurrentJobsPerWorkspace` 和实际 ResourceQuota/LimitRange。默认全局 100、每空间 10 会先于高档 Worker slot 限制任务；需要在整组阶梯前由管理员按环境能力确定，不能逐档偷偷提高后混称只改变 Worker。Runner 与 trial 均消耗配额，当前默认空间 request 2 CPU/4 GiB 只够一个默认规格 Runner 加一个 trial；不要以新增空间绕过配额规划。ACK/ACS、镜像、网络、数据库/保存目标能力和费用停止线都由操作者先核验，未通过档停止注入并处理在途任务，不据 slot 数宣布容量通过。

## Kubernetes 身份采样

在独立终端运行 [`kube_snapshot.py`](kube_snapshot.py)，读取同一批已接受 task ID，显式选择一个工作负载 namespace 和 kube context。多个空间分别运行、分别留档，累计计算观察流量。

```sh
python3 examples/agent-evaluation/load-test/kube_snapshot.py \
  --input "$LOAD_DIR/submit-1000.jsonl" \
  --output "$LOAD_DIR/kube-1000.jsonl" \
  --context "$KUBE_CONTEXT" --namespace "$WORKLOAD_NAMESPACE" \
  --job-selector 'app.kubernetes.io/managed-by=eruun' \
  --pod-selector 'eruun.io/task-id' \
  --duration 1800 --interval 30 --timeout 10 --rate 2
```

脚本只执行带 namespace/selector 的 `kubectl get`；Go template 在 kubectl 内摘取身份及状态，不输出 Pod env、完整 spec、任意 annotations 或 condition message。随后按输入 task ID 再过滤。Job 使用 `eruun.job/taskId` 注解或现有 task 标签；Runner/trial Pod 使用 `eruun.io/task-id`，Runner 额外带 `eruun.io/evaluation-runner=true`。仅在 ownerReference 的 kind/name/UID、namespace 和 task ID 全匹配时关联 Runner → Job、trial → Runner；同名 UID 改变记录 `recreated`，旧 UID 不会被替换名称误关联。前一完整快照中存在而本次缺失的对象记录 `missing_since_previous_sample`，这也可能是 selector 变化或采样时差，不能直接宣告删除。

旧 Job 可能尚无管理标签；对应 Runner 的 Job 关联会标 `missing`，不会退回全 namespace LIST 或每任务 GET。请以小批量、另行受控的精确名称/UID 核验补证；未知关联会降低证据覆盖。每轮 Job/Pod 读取并非同一原子快照，创建/删除间隙可能短暂不匹配。API 错误不会用空列表伪造资源消失，也不会覆盖上一个完整快照。

阶段一新 Runner 使用按需 Sandbox；保留原生 Pod 路径仅供旧执行配置完成。新路径采样应增加 `--sandbox-resource 'sandboxes.v1alpha1.agents.kruise.io' --sandbox-selector 'app.kubernetes.io/managed-by=eruun'`，预检先确认实际 CRD/GVR 与标签。为支持 24 小时保留，Sandbox 没有 Runner ownerReference；脚本通过固定的 `eruun.io/sandbox-runner-uid` 注解、同空间/task 和真实 Runner UID 记录 `runnerBinding=annotation_uid_matched`，与 Kubernetes `ownerLink` 区分。trial Pod → Sandbox 仍需真实 owner UID 匹配。Runner 已删除、替换或标签缺失时保持未知，不能按名字推断。此绑定只是采样证据，授权仍由 Eruun DB 和权威资源核验完成。脚本不创建任何资源；未启用 Sandbox 采样不代表其不存在。

开始前一并冻结 `resourceCreationQPS/resourceCreationBurst/maxStartingSandboxes`，以及账号配置中的空间 quota 和 Runner 工作目录 `runnerWorkStorageMiB`。默认 20 GiB/Runner 的 ephemeral request 在 10000 Runner 时合计约 200000 GiB，不能忽略这项调度资源，也不能与 trial 的 `storage_mb` 混算。可先为合成小任务选择更小的管理员配置，再固定整组阶梯；实际云盘分配、计费与节点 ephemeral 容量由环境预检核实。

### 故障与长稳的人工执行记录

完成单 Worker 阶梯后，按[阶段一矩阵](../../../docs/harbor-runtime-stage1-plan.md#63-实验矩阵)分别执行，避免一次同时改变多个条件：

1. 每轮保存新镜像 digest、所有角色副本/资源、scheduler 设置、有效 quota、任务包摘要和提交/观察 JSONL；记录监控时间源。将 synthetic task 的运行时间延长至大于爬坡和稳态窗口，不能用默认 60–300 秒验证两小时稳态。
2. 在持续运行的受控批次中分别重启 Worker、API、Controller、Scheduler；用既有 Deployment 操作逐个角色执行，记录实际故障与恢复时刻。观察原 Job/Sandbox/Pod UID 是否保持，旧 owner 写入是否被拒绝；重关联不能计为新资源创建。
3. 由环境负责人分别使测试 API、测试 DB 不可达 60/300/600 秒，保持 Kubernetes、Runner 和已有 trial 健康；同步采样资源创建计数，验证中断被检测后暂停新环境、已有 trial 可推进、恢复后完整结果及 terminal 确认。恢复后检查启动速率、Pending 占位、DB 锁等待和保存积压；不要将 600 秒注入加上人为额外恢复延迟当作“600 秒以内”。
4. 独立制造制品上传响应丢失、慢保存目标与采集不完整。核对 archive digest 不变、没有重复交付、其他结果可进展；保留案例检查 `retainUntil` 与云侧 `shutdownTime`，24 小时后 CR 消失且占位释放。等待时仍计费用；保留 Sandbox 可能仍有进程活动。
5. 长稳先做 72 小时子集，再做两周代表性任务；记录 Token 轮换、模型凭据有效期、磁盘峰值、结果大小、采样覆盖和未知数。Python token 轮换单测不能替代集群与 Provider 的长期证据。

这些操作需要操作者自己的隔离环境故障注入方式，仓库脚本不会修改网络、停数据库或扩大云资源。每轮先确定费用、429/错误率、内存和续租延迟停止线；触发后停止**新注入**，核对并处理在途和保留资源。未达到 10000 同时真实执行或缺少观测证据时，报告该轮最高通过档，不填写“万级通过”。

`--rate` 限制 kubectl 命令启动速率，默认串行，`--timeout` 同时限制 kubectl 请求和整个子进程，`--duration` 限制总采样窗口；发现、分页等操作可能令实际 Kubernetes HTTP 请求数高于 `kubectlCalls`。`--interval` 从上一完整轮次结束后起算，观察记录含每轮起止、累计调用数、task 覆盖和身份变化。输出以 `0600` 创建且禁止覆盖，退出码 1 表示存在读取失败/未完成轮次或没有完整样本，0 只表示采样完成。每个 task 的 `actualTrialExecution` 始终为 `unknown`：Pod `Running`、Ready 和身份链本身都不能证明 Harbor 正在实际执行评测。

运行两个观察脚本的本地测试：

```sh
python3 -m unittest discover -s examples/agent-evaluation/load-test -p 'test_*.py' -v
```

请求模板是同目录的 `evaluation.json`，使用 `type=job` 和 `traits.eval`，由平台 Runner 使用 Harbor 0.22.0，并选择 `oracle`、单 trial、900 秒 Job 执行预算及隔离数据库上的 `database/full` 保存目标。请求体省略 `workspaceId`；批量提交器仍用 `X-Eruun-Workspace-ID` 指定已授权空间，1000 个 Job 复用上传任务包的 `taskPackageId`。默认资源 Trait 为 Runner 和任务环境分别请求 1 CPU/2 GiB；并发增大前请按压测计划确认空间配额和节点容量。`--template` 可指定经过人工核对的其他请求模板，以便固定不同的资源或保存策略。压测结束后保留 JSONL、任务状态和监控记录，并按隔离环境流程清理镜像、数据库完整副本与 Kubernetes 资源；源归档的一天保留期不会自动删除数据库保存副本。
