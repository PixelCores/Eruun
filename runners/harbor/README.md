# Harbor Runner

> 状态：Current。此目录实现 Harbor `0.22.0` 的空间 Job Runner；完整 API 以实现 PR 对应的空间 Job 文档为准。

Runner 下载经过摘要验证的原生 Harbor `tar.gz` 任务包，调用真正的 `harbor run`，将框架生成的全部本地原始文件打包回传。它不构建用户镜像，不将任务包转换为 JSONL，也不替用户定义评分规则。

## 构建与验证

在仓库根目录运行：

```sh
docker build -t eruun-harbor-runner:0.22.0-local runners/harbor
python3 -m venv /tmp/eruun-harbor-test
/tmp/eruun-harbor-test/bin/pip install -r runners/harbor/requirements.txt
/tmp/eruun-harbor-test/bin/python -m unittest discover -s runners/harbor -v
```

需要 Python 3.12 或更新版本。测试包括真实 Harbor CLI 的配置解析及真实 ACK 后端的 Pod 生成；Kubernetes API 被替换为测试传输，不会连接用户集群或调用收费模型。未安装框架时，纯 Python 测试仍可运行，框架集成测试明确跳过。

已安装 Docker、kubectl 和 kind 时，可执行真正的 Kubernetes oracle 验收；脚本创建自己的 kind 集群和临时 kubeconfig，退出时清理该集群，不使用已有集群：

```sh
docker build -t eruun-harbor-task:1.0.0-local examples/agent-evaluation/harbor-task/environment
python3 runners/harbor/smoke_kind.py --result /tmp/harbor-smoke-results.tar.gz
```

该验收使用 restricted PodSecurity、无权限试验身份和本地镜像，真实运行 Harbor 到 oracle reward=1，并逐字节检查包含非 UTF-8 内容的制品及完整归档上传。它对状态入口注入连接中断和 HTTP 503，验证同 sequence 恢复；另运行 verifier 故意失败和被安全解包拒绝的制品场景，验证诊断回传及原始 Pod 保留。最后使已 claim 的 Runner OOM，并验证 replacement Pod 无法重新认领或下载数据。它使用模拟的平台传输服务，不覆盖生产 API 的鉴权、结果保存目的地、收费模型或生产集群故障矩阵。

镜像入口为 `python /opt/eruun/runner.py`，以 UID/GID `1000` 运行。挂载可写 `/work`，并提供 `POD_NAME`、`POD_UID` 的 Downward API 值。Runner 的 Kubernetes ServiceAccount 由平台限定权限；试验 Pod 使用另外的无权限 ServiceAccount。

## 运行协议

平台通过 `ERUUN_JOB_CONFIG` 注入 JSON，字段为：

```json
{
  "taskId": "<平台生成的 ID>",
  "namespace": "<已授权空间>",
  "datasetURL": "<平台任务包下载地址>",
  "datasetDigest": "<tar.gz 文件的 SHA-256>",
  "resultURL": "<平台原始结果上传地址>",
  "eventURL": "<平台 Runner 事件地址>",
  "token": "<本次任务绑定的传输能力>",
  "agent": {"name": "oracle"},
  "options": {"attempts": 1, "concurrency": 1},
  "resources": {"cpu": "1", "memory": "1Gi", "cpuLimit": "2", "memoryLimit": "2Gi"},
  "sandboxServiceAccount": "<平台创建的无权限身份>",
  "timeoutSeconds": 600,
  "transferTimeoutSeconds": 300,
  "finalizationTimeoutSeconds": 360
}
```

上述是内部 Runner 协议，不是用户提交接口。`token` 只用于平台的 Bearer 下载、上传和状态请求，不传给 Harbor 子进程，不写入输出。请求同时携带 `X-Eruun-Runner-Pod-Name` 和 `X-Eruun-Runner-Pod-UID`。平台验证任务、Pod、live Job UID、ExecutionKey、RunGeneration 与 Attempt，并在持久化后才返回 2xx。下载拒绝重定向，上传不跟随重定向。

Runner 启动后的第一个网络动作是同步提交 `claim`（`v1` sequence 1）；认领失败时绝不下载任务包、启动 Harbor 或上传结果。同一后台串行循环为后续 preparing/running/finalizing、15 秒 heartbeat 和有界 trial progress 分配 sequence，并对不确定 ACK 重放完全相同的事件。ACK `action=stop` 设置现有取消事件，终止 Harbor 进程组并进入收尾；状态入口故障不会重新启动 Harbor。服务端连续 60 秒未接收任何 Runner 事件时只使查询状态变为 stale。

`POST resultURL` 发送 `application/gzip`，头 `X-Eruun-Evaluation-Status` 为 `succeeded` 或 `failed`，并解析平台返回的持久化 artifact ID/digest。归档的根 `result.json` 是平台采集说明；`outputs/` 包含原始 Harbor 文件，包括原生 `outputs/run/result.json`、各 trial 的日志、轨迹、奖励、制品和框架附带文件。保留隐藏文件及二进制原文；安全的相对符号链接保留元数据，不解引用。危险链接、特殊文件、不可读文件记录在 `collectionErrors`，并设置 `collectionComplete=false`。

采集完整性同时覆盖试验 Pod 到 Runner 的下载和最终本地归档。适配器在任务目录与下载目录之外的 `/work/evaluation-*/collection` 原子记录每次下载、环境启动与收尾；Harbor 吞掉的日志/制品下载异常、过滤或解包失败、未结束的下载、缺失或损坏的记录、原生制品清单的失败/跳过条目都会使 `collectionComplete=false`。记录写入失败也不会降级为成功。正常的空制品目录允许完整采集；声明的制品实际缺失仍报告失败。

原生 `n_errored_trials`、未完成试验或缺失/损坏的结果都使执行失败，即使 Harbor 进程退出码为 0。正常完成且 reward 为 0 属于评测得到低分，仍是执行成功。SIGTERM/超时会终止 Harbor 进程组，并尝试上传已经收集的本地输出。框架被强杀、节点丢失或网络不可用时无法保证最后一次上传；平台不能因此声称输出已保存。

任务包上限为压缩 64 MiB、展开 256 MiB、10,000 个条目；结果归档上限为压缩 512 MiB、展开 2 GiB、10,000 个条目、路径 1,024 字节，API 文件清单最多 2 MiB。无法完整采集时明确设置 `collectionComplete=false` 并以失败状态退出；若连归档也无法生成，则回传 `diagnosticOnly=true` 的说明包。两者都不表示原始输出已经完整保存。本地原文件保留在 `/work/evaluation-*/outputs`，等待平台按 Pod 保留策略处理。下载总预算为 300 秒；进入 finalizing 后，结果采集、上传和 terminal 确认共享不超过 360 秒且不晚于任务启动时固定的绝对 deadline，重试复用完全相同的归档或事件字节。只有结果上传已确认、匹配 artifact 的 terminal 已 ACK 且执行与采集均成功，Runner 才返回 0。保存到用户所选目的地属于 API 接受原始结果之后的独立阶段。

## 框架与镜像边界

允许的 Agent 为 `codex`、`claude-code`、`terminus-2`、`oracle`；除 `oracle` 外必须提供 `agent.model`。模型凭据由平台将已授权 Secret 注入 Runner 环境，再由 Harbor 对应适配器使用。用户不能传任意 Python `import_path`、插件、环境 kwargs、Namespace、Pod 覆盖或 ServiceAccount。框架遥测关闭，也不启用 Harbor Hub 上传。

任务包支持位于根目录的单个任务或嵌套目录中的多个任务；每个任务需要 `instruction.md`、`task.toml`、`environment/Dockerfile`、`tests/test.sh`，并在 `[environment].docker_image` 引用显式版本或 digest 的预构建镜像。Dockerfile 仅作为原生环境定义保留；平台固定 `force_build=false`、`skip_image_check=true` 并拒绝构建入口。首版不支持 Compose、多步骤、独立 verifier 环境、GPU/TPU 或任务 MCP 集成。

`WorkspaceEnvironment` 是 Harbor 原生 ACK Kubernetes 后端的适配。它将 Harbor 的 setup 用户请求统一运行于已固定的 UID `1000`，避免 ACK 使用 `su root`。试验 Pod 强制 `runAsNonRoot=true`、禁止 privileged/提权、删除所有 Linux capabilities、使用 RuntimeDefault seccomp、不挂载 API Token，并继承执行截止时间、资源上限和空间 namespace。ownerReference 指向当前 Runner Pod UID，所属 Runner Pod 被删除时由 Kubernetes 回收试验 Pod；未完整采集而保留的 Runner 不会仅因控制器恢复就触发回收。

Harbor 的无条件删除关闭。每个 trial 收尾时，只有下载记录及原生制品清单均确认完整，适配器才删除该试验 Pod，及时释放运行资源供后续 trial 使用；完整原始字节此时已经位于 Runner 的 `outputs/`，最终归档或平台上传失败仍保留这些本地原件。采集失败的 trial 保留源 Pod，由既有 `activeDeadlineSeconds` 限制计算运行时间，再随 Runner 的保留策略回收。超时、取消或进程被强杀导致的未完成采集不能被标为完整。

适配器也将 ACK 的 tar 下载改为二进制 websocket 流并限制展开大小。Harbor 0.22.0 原始实现经 Kubernetes 客户端默认文本解码会破坏非 UTF-8 制品；这里保留原字节，使用 Python 的 `data` 安全过滤解包，并在最终归档时检查链接边界。

用户镜像必须预装 Agent 所需系统依赖，并让 UID `1000` 可写 `/home/agent`、`/app`、`/workspace`、`/logs`、`/tests`、`/solution`、`/installed-agent`。需要系统安装权限的命令会失败，平台不会放宽安全策略。建议在镜像内固定并预装 Codex/Claude Code 版本：Harbor 的适配器会识别已安装的二进制；未预装时其安装动作只能在用户可写路径成功。`terminus-2` 使用 Runner 中的 Harbor 实现驱动任务环境。

这些限制比 Harbor 上游支持范围窄。上游依据：[固定版本配置](https://github.com/harbor-framework/harbor/blob/v0.22.0/src/harbor/models/job/config.py)、[ACK 后端](https://github.com/harbor-framework/harbor/blob/v0.22.0/src/harbor/environments/ack.py)、[原生任务](https://www.harborframework.com/docs/tasks)。
