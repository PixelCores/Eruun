# 工作流超时状态区分说明

> 状态：Current。本文说明当前等待组件就绪超时时 `Timeout` 与 `Failed` 的区分规则。

## 背景

工作流在等待组件就绪时，历史行为会将超时统一标记为 `Timeout`。这会导致以下问题：

1. Pod 一直处于 Pending，确实属于等待超时。
2. Pod 已出现明确异常（如 `CrashLoopBackOff`），但最终也被标记为 `Timeout`，原始异常原因被弱化。

## 本次变更

### 状态判定规则

1. 若组件等待超时时 **仅表现为 Pending**（无异常线索），任务状态保持 `Timeout`。
2. 若组件等待超时时 **已观测到 Pod 异常**（`LastAbnormal` 非空），任务状态改为 `Failed`。

### 错误信息保留

当超时前已观测到 Pod 异常时，错误信息会包含异常原因（例如 `CrashLoopBackOff`），避免被通用 timeout 文案覆盖。

### Pod 失败诊断

Deployment 与 StatefulSet 类型组件在等待 Ready 失败时，会在 `eruun_job.error` 中写入格式化诊断信息：

1. `summary`：原始等待错误。
2. `workload`：资源类型、命名空间、资源名、`appId` 与组件名。
3. `pods`：匹配 Pod 的 phase、conditions、容器状态、重启次数、终止原因和 exit code。
4. `events`：匹配 Pod 的近期 Kubernetes Events。
5. `previous logs` / `current logs`：异常容器的 previous/current 日志片段。

当匹配 Pod 数量超过诊断上限时，诊断会先选择当前异常 Pod，再选择当前非 Ready Pod，然后选择有重启或历史终止证据的 Pod，最后才选择普通 Ready Pod；同一优先级内按创建时间较新优先。

诊断信息只写入当前 workflow job 的 `error` 字段，不写入组件运行态 `lastAbnormal`。组件 `lastAbnormal` 仍保持“最新运行态摘要”语义；普通容器已经恢复为 `Running` 且 `Ready`，或 init container 已成功 `Completed` 时，历史 `LastTerminationState` 不会让组件继续停留在 Failed。

用户取消或任务取消导致的等待中断不会追加 Pod 诊断；取消路径保留原始 `context.Canceled` cause，以便任务状态继续记录用户提供的取消原因。

Pod 诊断采集使用独立的短超时保护；如果列出 Pod/Events 或读取日志未能及时完成，任务会回退到原始等待错误，避免等待已超时后继续阻塞状态持久化和清理流程。

为避免错误字段过大，诊断文本会限制总长度；超出时保留开头摘要与末尾日志片段，并插入 `[diagnostics truncated]` 标记。

## 影响范围

1. 不新增 API 字段，不改现有响应结构。
2. 任务状态与错误信息更准确，影响 `workflow task status/stages` 的返回内容。
3. `eruun_job.error` 可能从单行错误变为多段格式化诊断文本。

## 验证命令

```bash
go test ./pkg/apiserver/infrastructure/informer -run TestWaitForComponentReadyTimeout
go test ./pkg/apiserver/utils/kube ./pkg/apiserver/event/workflow/job
```
