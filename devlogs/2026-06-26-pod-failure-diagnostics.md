# 2026-06-26 Pod Failure Diagnostics

## Context

Deployment 和 StatefulSet 组件等待 Ready 失败时，历史行为只把 waiter 观察到的 Pod 异常摘要写入 `eruun_job.error`。当容器进入 `CrashLoopBackOff` 或以 `Error` 退出时，排查通常还需要 Pod Events 与 previous/current logs，否则调用方只能看到 timeout 或简短状态原因。

## Decisions

- 在 deploy/store job 的 wait 失败路径采集 Pod 诊断，而不是新增公开 API。
- 诊断写入当前 workflow job 的 `eruun_job.error`，保持 `workflow task status/stages` 现有排障入口可直接展示。
- 不把长日志写入 `app_components.last_abnormal`；该字段继续表示组件最新运行态摘要，恢复后可清空。
- 普通容器已经恢复为 `Running` 且 `Ready`，或 init container 已成功 `Completed` 时，不再用历史 `LastTerminationState` 标记组件运行态失败，避免一次启动崩溃或 init 重试形成 sticky Failed。
- Pod 数量超过诊断上限时，先保留当前异常 Pod，再保留当前非 Ready Pod，然后保留有重启或历史终止证据的 Pod，避免已恢复但有历史重启的 Pod 挤掉当前阻塞 rollout 的 Pod Events 与 logs。
- 等待被用户取消时不采集 Pod 诊断，保留 `context.Canceled` cause，让 workflow job 继续写入用户提供的取消原因。
- Pod 诊断采集使用独立短超时；超时后回退原始等待错误，避免部署等待已经超时后仍卡在 Pod/Event/log 采集。
- 不新增 DB 字段或迁移；通过总长度限制保护 `error` 字段，超长时保留摘要开头和日志末尾并插入截断标记。
- 日志采集优先 previous logs，再采 current logs；读取失败只作为诊断片段记录，不覆盖原始 wait 错误。

## Notes

后续如果需要长期保存完整日志，应接入外部日志系统或单独设计归档表/对象存储，不应把完整容器日志无限制塞入 `eruun_job.error`。
