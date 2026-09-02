# Deployment Update VolumeMount Replacement

## 背景与需求

PaaS 版本更新会把组件目标 traits 发给 Eruun。一次日志路径变更中，PaaS 请求已经只为主容器发送新的挂载路径，但集群中的 Deployment 主容器同时保留了旧路径和新路径。原因是 Eruun 对已有 Deployment 使用 server-side apply；Kubernetes 对 `containers[].volumeMounts` 按 `mountPath` 合并，省略旧路径不一定会删除旧条目。

## 影响范围

- API: 无请求或响应字段变化。
- Domain: 无 DB 模型变化，版本更新仍完整覆盖组件 traits。
- DB: 无 schema 变化。
- Cache: 无变化。
- K8s: 已有 Deployment 更新从 apply patch 改为 read-modify-update；更新前按默认值归一化后的完整 PodTemplate Spec 和实际 update candidate 判断差异，更新时按目标 PodTemplate 替换受 Eruun 管控的 `containers`、`volumeMounts` 和 `volumes`，同时只保留 live 对象上的系统 label 与不可变 selector label。
- Workflow: workflow deploy job 的调和语义变化；未变化资源仍跳过更新。

## 技术选型与取舍

- 选择 Kubernetes `Update` 替代 server-side apply，用完整对象更新表达“目标 PodTemplate 替换”语义。
- 更新前重新读取 live Deployment，保留 `resourceVersion`、白名单系统 label 和不可变 selector，避免 selector 归一化或历史系统标签差异触发 immutable field 错误。
- 对已有 Deployment，单独的 desired selector 差异不再触发更新；selector 是不可变字段，差异判断必须以最终 update candidate 为准，避免每次 workflow 调和都发起无法收敛的 no-op `Update`。
- Deployment 差异判断不再维护手写的 container/volume 字段白名单，改为对默认值归一化后的完整 PodTemplate Spec 做语义比较，避免后续新增 trait 字段时遗漏触发更新条件。
- 不引入额外清理逻辑按路径删除旧挂载。删除逻辑绑定到单个字段会增加规则分支，而完整目标对象更新更符合当前 workflow 组件调和模型。

## 实现摘要

- `DeployJobCtl` 在已有 Deployment 发生差异时执行 get-update retry，每次冲突重试都基于最新对象重建 update candidate。
- `deploymentForExistingUpdate` 从 live Deployment 深拷贝开始构造 update candidate，顶层和 PodTemplate 的 labels/annotations 以 desired 为目标状态，只补回白名单系统 label；然后用 desired spec 替换目标状态，恢复 live selector，并把 selector labels 补回 template labels。
- `isDeploymentChanged` 使用同一 update candidate 比较 metadata 和 immutable selector，live-only 自定义 labels/annotations 会被清理；只有白名单系统 label 或 desired-only selector 的 live 差异不会导致每次调和都重复 update。
- 回归测试覆盖主容器旧 `/app/log` 被新 `/app/conf` 替换，同时 `logs-sidecar` 自己的 `/etc/vector` 和 `/app/log` 挂载保留；还覆盖同名 volume source 变化、`envFrom`、probe、securityContext、nodeSelector 等 PodTemplate 字段变化能够触发更新。

## 测试与验收

计划执行：

```bash
git diff --check
go test ./pkg/apiserver/event/workflow/job
go test ./... -race -cover
```

验收口径：版本更新后主容器只保留目标 traits 中的挂载路径和 volume source；日志 sidecar 保留自己的配置和日志挂载；Deployment selector 不变且更新不触发 immutable field 错误。

## 风险与后续

`Update` 会替换 Deployment spec 中 Eruun 生成的目标字段。当前实现保留 selector 和系统 label，但如果外部系统直接修改 Eruun 管控的 PodTemplate 字段或任意自定义 metadata，下一次 workflow 调和会按 Eruun 目标状态覆盖这些外部修改；这保证 `/version` 删除 `properties.labels` 时不会残留旧 label。
