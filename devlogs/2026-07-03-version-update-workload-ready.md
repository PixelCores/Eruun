# Version Update Workload Ready Observation

## 背景与需求

`/version` 的 `update` 操作可能修改 `webservice` Deployment 或 `store` StatefulSet 的 PodTemplate，例如 traits、replicas、properties 或 env。历史 Ready 观测目标只覆盖真实镜像变化，导致 traits 配错、资源配置错误或环境变量错误造成新 Pod 无法启动时，版本更新任务可能没有使用专门的 workload Ready 观测窗口表达这类风险。

同类风险也存在于 `restart` 资源动作：它会触发新 Pod，但历史 `version_restart` job 使用标准 20 分钟部署超时。对“5 分钟内判断是否能恢复”的版本更新语义来说，`restart` 应与普通 workload update 共用 `imageReadyTimeoutSeconds` 窗口。`add all` 全量部署仍保持完整部署语义，不纳入这个 5 分钟窗口。

## 影响范围

- API: 不新增字段；`imageReadyTimeoutSeconds` 字段名保持兼容，语义扩展为 workload 变更后的 Pod Ready 观测窗口。
- Domain: `/version` auto-exec 在提交前计算真实变更的 workload update 目标，并校验 workflow 覆盖；`restart` task 记录本次 Ready 观测窗口。
- DB: 无 schema 变化；继续复用 `workflow_queue.resource_action_info.imageReadyComponents`。
- Cache: 无变化。
- K8s: 不改变 Deployment / StatefulSet 调和器；它们仍在 deploy/store job 中等待目标 Pod Ready。
- Workflow: 更新 workflow task 只在所选 workflow 覆盖目标组件时创建；缺失目标组件时前置失败；`version_restart` job 使用 task-scoped Ready 观测窗口覆盖标准部署超时。

## 技术选型与取舍

- 保留 `imageReadyTimeoutSeconds` 和 `imageReadyComponents` 名称，避免 DB payload 和客户端字段迁移；用文档说明兼容字段的新语义。
- Ready 目标判断复用现有组件变更比较逻辑，避免单独维护 image、replicas、properties、env、traits 的重复分支。
- 对缺少 workflow 覆盖的 update 请求选择 fail-fast，而不是自动插入 deploy job。自动插入会改变用户显式 workflow 编排；前置失败能保证 callback success 仍代表所选 workflow 覆盖的目标已经完成 Ready 观测。
- `add` 组件沿用原有目标记录语义，不在本次 PR 中扩大范围；新增组件的 workflow step 同步仍由现有版本更新流程处理。
- `restart` 不写入 `imageReadyComponents`，因为它不是 deploy/store deploy step；它复用同一个 `imageReadyTimeoutSeconds` 字段作为 `version_restart` job 的等待窗口。
- `add all` 不记录 Ready 目标，也不覆盖 deploy/store deploy job 超时；全量部署仍走标准部署超时。
- 对同镜像但 traits / env / properties 引起 PodTemplate 变化的普通 deploy/store deploy，选择复用 `eruun.job/taskId` 作为本次目标 PodTemplate 标记，并在 waiter 中通过 `ExpectedAnnotations` 过滤。没有采用预计算 `pod-template-hash`，因为该 hash 由 Kubernetes controller 生成；没有只看 `observedGeneration`，因为它不能证明目标 Pod 已 Ready。
- task 标记只在新建 workload 或已有 workload 的 PodTemplate 真实变化时写入，不在 replicas-only、顶层 metadata-only 或 no-op 调和中制造额外滚动。

## 实现摘要

- `/version` 在 auto-exec 路径中计算真实变更的 workload Ready 目标，并把目标写入 task-scoped `ResourceActionInfo`。
- 对真实变更的 workload `update` 目标，要求所选 workflow 的 `deploy` 组件步骤或子步骤覆盖该组件；否则返回 workflow config 错误且不提交版本、组件或 task。
- Deployment / StatefulSet job builder 继续通过 task-scoped `imageReadyComponents` 覆盖对应 job timeout，并复用现有 informer Ready 等待。
- Deployment / StatefulSet deploy job 在新建 workload 或更新目标 PodTemplate 时写入 `eruun.job/taskId` 到 PodTemplate，并把同一 annotation 传给 `WaitForComponentReadyWithOptions`；同镜像旧 Pod 因缺少本次 task 标记不会再满足 Ready 条件。
- `restart` task 会持久化 `ImageReadyTimeoutSeconds`，`version_restart` job builder 使用该值覆盖默认部署超时，并继续等待本次 `kubectl.kubernetes.io/restartedAt` 注解对应的新 Pod Ready。

## 测试与验收

计划执行：

```bash
git diff --check
go test ./pkg/apiserver/domain/service/application ./pkg/apiserver/event/workflow
go test ./... -race -cover
```

验收口径：`webservice` / `store` 的真实 update 变更会进入 Ready 目标；restart 使用 `imageReadyTimeoutSeconds` 覆盖 `version_restart` job 超时；`add all` 不写入 Ready 目标；no-op update、`config` / `secret` 更新不会进入 Ready 目标；缺少 workflow 覆盖的 workload update 在提交前失败。

## 风险与后续

旧 workflow 如果没有包含被 `/version update` 修改的 workload 组件，过去可能提交成功但没有观测目标组件 Ready；现在会显式失败。调用方需要补齐 workflow deploy 步骤，或改用覆盖目标组件的 workflow。
