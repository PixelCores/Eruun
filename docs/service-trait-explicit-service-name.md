# Service Trait 显式命名规则更新

> 状态：Current。本文说明当前 service trait 显式命名和模板服务名重写规则。

## 变更说明

`traits.service[].name` 现在采用以下规则：

1. 当 `name` 非空时，直接作为最终 Service 的 `metadata.name`。
2. 当 `name` 为空时，使用默认资源命名体系：非 shared 组件按规范化后的 `appName + componentName` 生成；带 `traits.share` 的组件按 `componentName` 生成。
3. 当组件来自模板克隆时，非空 `name` 会先实例化为当前应用可读的 Service 名：
   - 当 `type` 字面值为 `internal` 时，使用 `<appName>-<oldServiceName>`，例如应用 `game-db` 中的 `mysql-master` -> `game-db-mysql-master`。空 `type` 或 Kubernetes 原生 `ClusterIP` 虽然运行时也会归一化为 internal，但不触发这条短命名规则。
   - 包含模板组件名的完整 `-` 分隔片段时，将模板组件名替换为克隆后的组件名，例如 `tmp-mysql-8-master` -> `game-db-mysql-master`。
   - 如果模板组件名本身包含 `-`，仍按完整片段匹配，例如 `mysql-8` -> `new-mysql-8` 时，已经是目标名的 `new-mysql-8-master` 不会再次变成 `new-new-mysql-8-master`。
   - 不包含模板组件名时，使用 `<newComponentName>-<oldServiceName>`，避免多个克隆应用共享同一个固定 Service 名。
   - 该实例化默认按组件作用域执行；但字面 `type: internal` 按应用作用域执行，多个模板组件都使用同一个 internal `name` 时会生成重复 Service 名并被校验拦截。

对应地，Ingress 的默认后端 serviceName 也遵循同样规则：

1. 优先使用 `traits.service[].name` 的显式值。
2. 若未显式配置，回退到默认命名规则。
3. 模板克隆会同步改写 Ingress backend 中引用到的模板 Service 名。
4. 如果同一组件声明了多个非 `external` 的 `traits.service[]`，Ingress route 不能省略 `backend.serviceName`，必须显式指定要转发到哪一个 Service。
5. 当 route 显式指定 `backend.serviceName` 但省略 `backend.servicePort` 时，端口会优先从同名 Service trait 推导，避免多个 Service trait 下误用第一个 Service 的端口。

模板克隆还会同步改写模板里引用显式 Service DNS 的环境变量和命令片段，包括组件 `properties.env`、`properties.command`、init 容器 env/command、sidecar env/command/args，以及 ExternalName Service 的 `traits.service[].externalName`。裸 Service 名不会在自由文本中重写，避免误改 URL scheme、命令名或普通参数。`properties.labels` 以及 Service trait 的 selector/labels 值仍会按精确值重写；若 selector 使用保留键 `eruun.io/component-name`，该值会按 bounded component label 规则规范化，以匹配 Pod label。

文本改写只匹配明确 DNS 形式的 Service host，不会改写裸 Service 名或更长单词里的子串，例如 `mysql`、`mysql-mastering` 或 `primary-role` 会保持原样。
如果自由文本中的 DNS host 对应多个同 namespace 的模板 Service 候选，例如多个组件都声明 `name: primary` 且文本中出现 `primary.default.svc`，系统不会在这里 fail fast，也不会猜测应该改写到哪个克隆 Service。该值会保持原样，因为它可能是用户有意指向共享 Service、旧环境 Service，或通过切流机制在新旧 Service 之间切换。需要强约束引用模板内某个 Service 时，应使用无歧义的 namespace-qualified DNS，或使用 Ingress backend 等 typed reference。

## 校验规则

当 `traits.service[].name` 非空时，会执行 DNS-1123 校验（小写字母、数字、`-`，且首尾必须是字母或数字）。
模板克隆会在名称实例化后再次校验，超过 Kubernetes 63 字符限制或格式不合法会直接报错，不做截断或 hash，避免生成不可预测的 Host。
如果同一次创建请求中多个 `traits.service[]` 会生成同一个目标命名空间/name，也会直接报错，避免后续 Kubernetes Service 互相覆盖。这个校验覆盖普通组件、同一模板的多个组件、不同模板 ID 展开的组件，以及 `traits.service[]` 存在但未显式填写 `name` 时按默认规则生成的 Service 名。
由 `properties.ports` 自动派生的默认 Service 属于默认资源命名体系：非 shared 组件绑定到规范化后的 `appName + componentName`，shared 组件绑定到 `componentName`；模板版本不参与运行时 Service 命名。创建阶段会把默认 Service 与显式 `traits.service[]` 一起纳入资源名冲突校验。
如果多个模板组件复用同一个显式 Service 名，其他组件再通过 Ingress backend 等 typed reference 引用该短名时也会直接报错，并在错误信息中列出候选 Service 名；这类引用需要改成无歧义的 Service 名。
`properties.labels`、`traits.service[].labels`、`traits.ingress[].label` 不能覆盖 Eruun 托管 label（例如 `eruun.io/app-id`、`eruun.io/component-name`、`eruun.io/component-id`、`app.kubernetes.io/managed-by`）；真实生成资源时也会以托管 label 为准，避免用户 label 破坏 Pod/Service/Ingress 的选择关系。
不合法名称会在应用校验阶段直接报错。

## 兼容性影响

该变更是行为切换：

1. 旧行为：`name: mysql-master` 生成带系统前缀和 appID 的 Service 名。
2. 新行为：`name: mysql-master` 直接生成 `mysql-master`。
3. 模板克隆行为：模板内 `name: mysql-master` 克隆为组件 `game-db-mysql` 时，最终生成 `game-db-mysql-master`。
4. 模板克隆中的字面 internal 行为：应用 `game-db` 内模板 `name: mysql-master` 最终生成 `game-db-mysql-master`，不会额外拼入克隆组件名。

显式 `traits.service[].name` 和未填写 `name` 的默认 Service 都使用当前资源命名体系。若依赖旧的 `svc-`/`appID` 命名，需要按新资源名重新创建或清理旧资源。

## 验证命令

```bash
go test ./pkg/apiserver/event/workflow/...
go test ./pkg/apiserver/workflow/traits -run "TestApplyIngressDefaultsService"
go test ./pkg/apiserver/domain/service -run "TestValidationService_TryApplication_(ValidServiceTrait|InvalidServiceTraitNameFormat)"
```
