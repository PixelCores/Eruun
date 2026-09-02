# Kubernetes Label Value Normalization

> 状态：Current。本文说明 Eruun 渲染 Kubernetes 资源时对 `metadata.labels` 和 selector label value 的规范化规则。

## 行为

Eruun 会在生成 Kubernetes 资源前，统一规范化写入 `metadata.labels` 的 value。label key 保持不变；key 具有 Kubernetes 和 selector 语义，非法 key 仍按原有路径 fail-fast。

规范化范围包括：

- `properties.labels` 写入 workload、Service、ConfigMap、Secret、Job、CronJob 等生成资源时的 label value。
- `traits.service[].labels` 的 label value。
- `traits.service[].selector` 的 selector value 会使用 selector 专用规则：已知匹配 Eruun 生成 Pod label 的 key 会规范化到生成值；其他 key 会保留已经合法的 Kubernetes label value，只修复非法值。
- `traits.ingress[].label` 的 label value。
- RBAC trait 生成的 ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding label value。

Eruun 管理标签仍走既有管理标签路径，例如 `app.kubernetes.io/managed-by`、`eruun.io/app-id`、`eruun.io/component-id`、`eruun.io/component-name`。

## 规则

- 空 label value 是 Kubernetes 合法值，会保留为空字符串。
- 写入 `metadata.labels` 的非空 value 会转成小写 RFC 1123 风格：非法字符和空白折叠为 `-`，首尾 `-` 会被移除。
- 超过 63 字符的 value 会截断并追加稳定 hash 后缀，保证同一输入得到同一输出。
- native 组件的 `traits.service[].selector` 如果包含 `eruun.io/app-id`、`eruun.io/component-id` 或 `eruun.io/component-name`，这些值会绑定到当前目标组件生成的身份标签，不能继续引用源应用身份；adopted 组件保留源工作负载 selector。
- `traits.service[].selector` 中的 `app.kubernetes.io/managed-by`，以及 key 同时存在于 `properties.labels` 的 selector value，会按 Eruun 生成 Pod label 的同一规则规范化。
- 其他 `traits.service[].selector` value 如果已经是 Kubernetes 合法 label value，会原样保留，例如 `version: v1.2.3` 和 `track: canary_A`；只有非法 value 才会被规范化。

示例：

```text
penalty shootout 2026-m2606241344ccufxh-backend
```

会渲染为：

```text
penalty-shootout-2026-m2606241344ccufxh-backend
```

如果输入已经是合法但无分隔的值，例如 `penaltyshootout2026-2606241344ccufxh-frontend`，通用 label value 规范化会保留其词边界现状，不会推断成 `penalty-shootout-2026`。需要这种业务 slug 时，应从上游显示名或专门 slug 字段生成带分隔符的值。
