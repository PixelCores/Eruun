# Template Service Rewrite 冲突示例

这些示例记录模板克隆时 Service 名和 DNS 引用 rewrite 可能遇到的冲突，以及修复后期望的行为。

## namespace-qualified-ambiguous-service.json

场景：

- 模板里有两个组件都声明了显式 Service 名 `primary`。
- `mysql` 组件位于 `default` namespace，克隆后 Service 为 `new-mysql-primary.default`。
- `redis` 组件位于 `redis-ns` namespace，克隆后 Service 为 `new-redis-primary.default`。
- 克隆目标 namespace 是 `default`。
- `gateway` 组件通过 namespace-qualified DNS 分别引用：
  - `primary.default.svc.cluster.local`
  - `primary.redis-ns.svc.cluster.local`

曾经的问题：

- `primary` 是歧义短名，不能在全局 rewrite 中直接映射。
- 但如果给所有歧义 Service 都额外添加 target namespace alias，`redis-ns` 的 `primary` 会尝试把 `primary.default` 映射到 `new-redis-primary.default`。
- 同时，真正来自 `default` 的 `primary` 已经把 `primary.default` 映射到 `new-mysql-primary.default`。
- 结果 clone 会提前失败：`template reference "primary.default" maps to both ...`。

修复后的期望：

- 歧义 Service 不添加 target namespace alias。
- 带 source namespace 的 DNS 可以精确 rewrite：
  - `primary.default.svc.cluster.local` -> `new-mysql-primary.default.svc.cluster.local`
  - `primary.redis-ns.svc.cluster.local` -> `new-redis-primary.default.svc.cluster.local`
- 裸短名 `primary` 仍然是歧义引用；跨组件 typed reference 应 fail fast。
- `properties.env`、`properties.command`、init/sidecar env/command/args 这类自由文本中无法判定的 DNS host 保持原值，不 fail fast。它可能是用户有意指向共享 Service、旧环境 Service，或通过切流机制在新旧 Service 间切换。

## request-wide-service-name-collision.json

场景：

- 模板组件 `mysql` 声明 `traits.service[].name: primary`。
- 克隆请求把它实例化为组件 `new-mysql`，最终 Service 名是 `new-mysql-primary`。
- 同一个创建请求里，普通组件 `worker` 也显式声明 `traits.service[].name: new-mysql-primary`。

曾经的问题：

- 克隆阶段只检查当前模板展开批次里的 Service 名。
- 普通组件或另一个模板 ID 生成的同名 Service 不会被提前发现。
- 后续部署会对同一个 namespace/name 应用两个 Kubernetes Service，可能互相覆盖 selector、ports 或 ExternalName。

修复后的期望：

- 在模板展开完成后，对整个创建请求里的 `traits.service[]` 做最终 namespace/name 唯一性校验。
- 如果出现重复 Service 名，创建请求直接失败，并在错误信息中指出冲突的组件和 service index。
- 由 `properties.ports` 自动派生的默认 Service 仍走默认命名体系，名称绑定当前 AppID，不属于这个显式 Service trait 冲突示例。

## external-name-template-service-dns.json

场景：

- 模板组件 `mysql` 生成 Service DNS `mysql.default.svc.cluster.local`。
- 模板组件 `proxy` 声明 ExternalName Service，`externalName` 指向这个模板内 Service DNS。

曾经的问题：

- 克隆时会把目标 Service 重写为 `new-mysql.default.svc.cluster.local`。
- 但 `traits.service[].externalName` 没有参与 DNS rewrite，仍然指向旧的 `mysql.default.svc.cluster.local`。

修复后的期望：

- `externalName` 中明确指向模板 Service 的 DNS 会同步 rewrite。
- 无关外部域名，例如 `example.org`，保持原值。

## 带连字符组件名的 already-target 场景

场景：

- 模板组件名是 `mysql-8`。
- 克隆目标名是 `new-mysql-8`。
- 模板 Service 已经写成目标形式 `new-mysql-8-master`。

曾经的问题：

- 旧判断只按单个 `-` 拆分，无法把 `mysql-8` 识别为完整片段。
- Service 名会被再次重写成 `new-new-mysql-8-master`。

修复后的期望：

- `mysql-8` 按完整 `-` 边界片段匹配。
- 已经是目标形式的 Service 名保持 `new-mysql-8-master`。
