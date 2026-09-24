# Harbor 评测恢复

> 状态：Current / Experimental。本文描述可选的恢复实现；本地协议测试和原生 CLI 会话实验不等于真实 ACS 验收。真实集群测试由维护者后续执行，默认不启用恢复。

## 启用与版本

独立 Workspace Job 与 Application 的 `job` 组件使用相同的 `traits.eval.recovery`：

```json
{
  "agent": "codex",
  "model": "openai/<model>",
  "recovery": {
    "agentVersion": "0.154.0",
    "replaySafe": true,
    "checkpointIntervalSeconds": 300
  }
}
```

上面是 `traits.eval` 的部分字段；仍需按 [Workspace Job API](workspace-jobs-api.md) 提供 `env`、任务包及 Secret 引用。`recovery` 省略时沿用普通 Harbor 执行。`checkpointIntervalSeconds` 默认 300，范围 60–3600；它是 Agent 分段运行的目标时长，实际恢复点间隔还包括 setup、verifier、采集、其他 trial 和云快照等待，不能当作 RPO 保证。

| Harbor | Agent | 必填 agentVersion |
| --- | --- | --- |
| 0.22.0 | `codex` | `0.154.0` |
| 0.22.0 | `claude-code` | `2.1.281` |

恢复模式拒绝其他 Agent、版本以及未明确设置 `replaySafe: true` 的请求。CLI 安装固定版本，运行时再次检查实际版本。任务镜像必须包含 Linux `/proc` 与 Python 3，允许 UID 1000 安装和运行对应 CLI。恢复模式要求 Sandbox 中没有其他未受监督的后台进程；不满足时任务明确失败，不降级为从头重跑。

`replaySafe` 是任务作者对重放的声明，不是平台自动证明幂等：恢复会回到最近的完整点，未确认完成的工具命令和该点之后已经生效的外部调用可能再次执行。平台不提供外部副作用的 exactly-once，也不回滚外部数据库、网络服务或云 API。

## 恢复点与恢复执行

Runner 使用有界并发批次执行原始 trial 计划。每个 Agent 片段结束时停止原生 CLI 进程组，检查整个可见 PID namespace 是否仍有非基线进程，并同步文件。setup 和 verifier 完成前不发布该批恢复点；所有未完成 trial 都停在 Agent 边界后，冻结成员集合、原生会话 ID、剩余 Agent 时间和 Runner 材料。

材料保存在既有制品分块存储中，包括原始 trial 配置、已完成结果与采集状态、文件清单和摘要。控制 token、旧 Sandbox 控制文件、临时下载地址不作为恢复材料。新 Runner 重新获取环境凭据和执行 claim；任务自身产生的文件仍可能包含敏感内容，应按评测制品权限管理。

API 从已认证执行和 trial 映射获得真实 Sandbox/Pod UID，然后创建 `agents.kruise.io/v1alpha1` Checkpoint。只有全部成员快照成功、供应商返回有效快照 ID 且材料完整时，恢复点才成为 `ready`。部分失败、身份变化、超时或旧 claim 均不能发布完整点。同一执行的未完成快照操作串行化，创建使用现有资源创建预算。

Runner 或执行基础设施失败且没有已提交的业务终态时，Worker 可以选择最近完整点。每项任务的执行链最多恢复三次；恢复沿用原执行绝对截止时间，setup、排队、快照和故障停顿都不会增加预算。已明确报告的 Agent/verifier 失败、取消和超时不会自动重放。

新执行有独立 ExecutionKey、Runner token、JobInfo 和 Sandbox 身份。恢复预留与旧能力撤销在现有 Workflow ownership 事务下持久化；Worker 接管后继续同一个预留。旧执行的事件、结果和新快照请求不能写入新执行。

启动副本前必须确认源 Runner 和源 Sandbox 的**同一 Pod UID 的全部容器已有可确认的终止状态**。服务端对源 Sandbox 请求 `shutdownTime` 并等待终态。心跳过期、Pod 不存在、UID 被替换、NodeLost/未知容器状态或网络分区均不能单独证明隔离，当前实现会阻止恢复并报告隔离未确认。此限制意味着并非所有节点丢失场景都能自动恢复；ACS 的实际停止和终态保留行为仍需实测。禁止通过强制删除源 Pod 来绕过这项门禁。

克隆保留源 Sandbox 的 Pod 模板 spec，使用供应商恢复注解和快照 ID；不会重新计算模板中的 `activeDeadlineSeconds`。新执行的逻辑期限由服务端和 Runner 另行执行。云侧默认化、runtime 注入与卷覆盖需要 ACS 验证。已完成 trial 不重跑；未完成 trial 跳过破坏性的安装/setup，通过保存的明确 session ID 继续原生会话；尚未开始的 trial 正常创建新环境。

## 生命周期与部署

每个源执行最多保留两个完整点，并限制未完成操作数量；Runner 单份恢复材料压缩上限 64 MiB，展开上限 256 MiB，最多 10,000 条目。恢复点的保留上界为原执行截止时间后 24 小时；任务终态或达到原截止时间后可提前回收，到期点不能被新恢复选择。引用中的点受保护，失败或仍在运行的云快照继续追踪到可安全回收状态。清理依赖 Controller；Controller 中断会延迟物理回收，不代表云资源已经删除。

空间删除会拒绝尚未清理的恢复点。排空运行/恢复执行后，等待维护循环回收快照和材料，再删除空间。不要把 Running Checkpoint CR 删除当作取消供应商快照任务。

升级先部署数据库迁移、API/Controller 的 Checkpoint RBAC 和所有 Server 读方，再部署含新恢复模块的 Runner 镜像，最后通过 `traits.eval.recovery` 开启任务。使用新的明确镜像 tag/digest；回滚前停止新准入、排空恢复执行及未清理恢复点，旧版本严格解码器不能读取新增恢复字段。gRPC 使用同一个 `EvaluationTrait.recovery` 契约。

Runner 内部接口为 `POST/GET /api/v1/job-runners/{taskID}/checkpoints/{checkpointID}`，以及 `GET .../{checkpointID}/material`。它们只接受执行绑定的 Runner 身份，不是用户管理接口，也不通过 gRPC 暴露。

## 验证与待办

在项目根目录运行：

```bash
python3.13 -m venv /tmp/eruun-recovery-venv
/tmp/eruun-recovery-venv/bin/python -m pip install -r runners/harbor/requirements.txt
LITELLM_LOCAL_MODEL_COST_MAP=True /tmp/eruun-recovery-venv/bin/python -m unittest discover -s runners/harbor -p 'test_*.py'
go test ./pkg/apiserver/jobs/... ./pkg/apiserver/event/workflow/... -race
```

上述测试默认不运行真实 CLI fixture；设置 `ERUUN_TEST_NATIVE_CODEX` 和 `ERUUN_TEST_NATIVE_CLAUDE` 为固定版本可执行文件路径后，可运行完整回环命令测试，具体命令见 [Runner 文档](../runners/harbor/README.md#实验性原生会话恢复)。

原生 CLI 本地会话探针及上游 Harbor 恢复粒度实验见 [第二阶段计划](harbor-runtime-stage2-plan.md#21-s2-1-本地能力实验)。测试未证明任务工具、完整生产镜像和真实 ACS 能端到端恢复。

真实 ACS 验收待维护者单独执行：

- 固定地域、CRD、ACS 组件、Runner/任务镜像及两个 CLI 版本，验证快照和克隆、Pod spec 一致性、可写层及挂载卷覆盖。
- 多 trial 混合完成/未完成恢复，删除旧 Runner 工作目录，确认 completed trial 不重跑、session ID 保留、verifier 和采集结果完整。
- Runner/节点故障、旧实例网络分区、停止确认、取消、原 deadline、重复恢复及 Worker/Controller 接管。
- 快照部分失败、供应商调用超时、材料损坏、凭据轮换、引用保护、过期回收、空间删除。
- 记录 RPO/RTO、暂停时长、材料大小、快照配额和成本，并在阶段一背景负载下验证容量；目前没有已验证的生产 SLO。
