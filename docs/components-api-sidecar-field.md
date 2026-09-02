# Components API 组件详情字段扩展

> 状态：Current。当前路由为 `GET /api/v1/applications/:appID/components`。

## 背景

`GET /api/v1/applications/:appID/components` 原有响应已经返回组件基础信息、`properties`、`traits`、`sidecars` 和外部访问链接。
为了让前端和集成方基于一个 `APP_ID` 直接拿到组件关联的服务入口、Ingress 路由、资源配置和密钥引用信息，本次在组件对象顶层补充汇总字段。

## 统一契约来源

当组件详情字段涉及跨层语义（API/Domain/DB/Cache/K8s）时，以以下文档作为统一契约基线：

- `docs/core-module-boundary-and-cross-layer-contracts.md`

本页仅描述 `GET /api/v1/applications/:appID/components` 的接口行为与字段语义，不重复定义跨层优先级、编码边界和事实源规则。

## 接口变更

- 方法：`GET`
- 路径：`/api/v1/applications/:appID/components`
- 变更：响应 `data.components[]` 新增或继续保留以下可选字段：
  - `sidecars []`：组件边车配置，语义与 `traits.sidecar` 一致。
  - `services []`：组件对应的 Service 端口信息。
  - `ingresses []`：组件对应的 Ingress 信息和路由后端。
  - `resourceConfigs []`：组件主容器、init 容器、sidecar 容器的资源配置集合。
  - `credentials []`：组件引用的 Secret 账号密码等密钥信息。

## 字段说明

### services

`services` 来自组件的 `traits.service`；当组件未显式配置 service trait 但 `properties.ports` 有端口时，会按现有组件服务命名规则返回默认内部 Service 信息。

每个 Service 包含：

- `name`
- `namespace`
- `type`
- `headless`
- `externalName`
- `ports[]`

`ports[]` 包含：

- `name`
- `port`
- `targetPort`
- `protocol`

### ingresses

`ingresses` 只会对当前 workflow 实际会部署 Ingress 资源的组件类型生效，并基于对应的 `traits.ingress` 补齐名称、命名空间和默认后端服务。像 `config`、`secret`、`cloudjob` 这类不会走 Ingress trait 落地链路的组件，即使请求体里带了 `traits.ingress`，这里也会返回空。`routes[].host` 优先使用路由级 `host`；当路由级 `host` 为空且 Ingress 级 `hosts` 有值时，按 `hosts` 展开路由摘要；两者都为空时保留无 host 规则。

其中 `ingresses[].name` 和 `ingresses[].namespace` 表示 workflow 实际会部署的 Ingress 资源标识，可用于定位集群中的 Ingress 对象；它们仍然不应视为对外访问入口。调用方如果需要对外可访问的固定路由，应优先使用：

- `externalLinks` 中 `type=ingress` 的链接
- `ingresses[].routes[]` 里的 `host + path`

每个 Ingress 包含：

- `name`
- `namespace`
- `ingressClassName`
- `annotations`
- `tls`
- `routes[]`

`routes[]` 包含：

- `host`
- `path`
- `pathType`
- `serviceName`
- `servicePort`
- `weight`
- `headers`
- `rewrite`

### resourceConfigs

`resourceConfigs` 汇总以下位置的 `traits.resources`：

- `scope: "main"`：组件主容器。
- `scope: "init"`：`traits.init[].traits.resources`。
- `scope: "sidecar"`：`traits.sidecar[].traits.resources`。

每条记录包含：

- `scope`
- `name`
- `cpu`
- `memory`
- `cpuLimit`
- `memoryLimit`
- `gpu`

### credentials

`credentials` 汇总组件及其 init、sidecar 中引用 Secret 的配置：

- `traits.envs[].valueFrom.secret`
- `traits.envFrom[]` 中 `type` 为 `secret` 的引用
- `traits.storage[]` 中 `type` 为 `secret` 的引用

每条记录包含：

- `source`：引用来源，例如 `component.envs`、`component.init[init-db].storage`、`component.sidecar[metrics].envFrom`。
- `envName`：来自环境变量时的变量名。
- `secretName`：引用的 Secret 组件名。
- `key`：引用的 Secret key；整包 Secret 引用会按 key 拆成多条记录。
- `value`：解析到的字面值；仅当同一应用、同一命名空间的 `secret` 组件中存在该 key，且值为非空字符串时才会返回。
- `resolved`：是否从同一应用、同一命名空间的 `secret` 组件中拿到了可展示的非空字面值。

当前解析范围是同一应用内的 `secret` 类型组件，并使用其 `properties.secret` 作为 Secret 数据源。`properties.secret` 只承载文本型 Secret，所有值都按字面量处理，即使内容看起来像 base64 也不会解码。无法解析 Secret 或 key，或者 key 对应值为空字符串时，会返回 `resolved: false`，且不返回 `value`。

`secretMeta` 已删除，不再区分编码态/明文态，也不再要求调用方在写回时透传额外元数据。导入或转换 Kubernetes Secret 时，仅支持可表示为 UTF-8 文本的值；非 UTF-8 二进制 Secret 会被拒绝。

跨层编码与优先级约束参见：`docs/core-module-boundary-and-cross-layer-contracts.md` 中 `properties.secret[*]`、`credentials.value`、`k8s secret payload` 相关条目。

## 安全说明

`credentials.value` 会返回明文密钥值。调用方必须将该接口响应按敏感数据处理，避免写入普通日志、埋点或前端持久化存储。服务端当前不会在转换日志中打印这些值。

## 兼容性说明

- `properties` 和 `traits` 继续保留，不做删除或重命名。
- 新增字段均为 `omitempty` 字段；组件没有对应配置时不会出现在响应中。
- `sidecars` 保持与 `traits.sidecar` 相同语义，历史调用方不受影响。

## 示例

请求：

```bash
curl "http://127.0.0.1:8000/api/v1/applications/app-123/components"
```

响应示例见：`examples/components/list-application-components-response-sidecar.json`
