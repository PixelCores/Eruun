# Gateway Trait 抽象化实施方案（Ingress -> Istio/多 Provider）

> 状态：Draft / Proposal。本文是 Gateway Trait 演进实施计划，描述计划实现的 Gateway Trait 抽象；当前生产行为仍以现有 Ingress Trait 代码为准。

> 示例说明：本文代码、JSON、YAML 和路由均为概念伪代码，不代表当前已注册接口或可直接执行的配置。

## 1. 文档信息

- 版本：v1.1
- 更新时间：2026-05-15
- 适用范围：`Eruun` API Server traits / validation / workflow / convert / assembler 链路
- 目标读者：实现该能力的后端工程师、评审者、未来新增 provider 的维护者

## 2. 背景与目标

当前入口能力以 `traits.ingress` 为主，用户配置直接绑定 Kubernetes Ingress 资源。这个设计能覆盖基础 HTTP 入口，但会把“对外路由意图”和“底层网关实现”耦合在一起：一旦集群使用 Istio、Gateway API 或其他网关，用户配置和服务端转换链路都需要感知具体实现。

本方案新增统一入口抽象 `traits.gateway`。用户只描述“哪个 host/path 转发到哪个 service/port”，底层由 provider 适配器生成具体 Kubernetes 资源。

核心目标：

1. 新增统一 Gateway Trait，抽象现有 Ingress 语义。
2. 首期 provider 支持 `ingress` 与 `istio`。
3. 保留 `traits.ingress`，旧应用不需要迁移即可继续运行。
4. `traits.gateway` 与 `traits.ingress` 并存时，以 `traits.gateway` 为准。
5. 不引入 Istio Go SDK，Istio 资源使用 `unstructured.Unstructured` 构建。

非目标：

1. 不负责安装、升级或管理 Istio 控制面。
2. 不在首期生成 Istio `Gateway` 资源。
3. 不在首期实现 Gateway API、Traefik、Kong、APISIX 等 provider。
4. 不做数据库离线批量迁移脚本。
5. 不改变现有 `traits.ingress` 的公开 JSON 契约。

## 3. 关键决策

| 决策点 | 结论 |
| --- | --- |
| 抽象字段名 | `traits.gateway` |
| 兼容策略 | `gateway` + `ingress` 双轨兼容，`gateway` 优先 |
| 首期 provider | `ingress`、`istio` |
| provider 默认策略 | `auto`：自动探测 Istio CRD，具备 Istio 条件时选 `istio`，否则选 `ingress` |
| Istio Gateway 管理边界 | 引用平台预置 Gateway，只生成 VirtualService |
| Istio TLS 语义 | TLS 在预置 Gateway 上终止，`gateway.tls` 不映射到 VirtualService |
| Istio 依赖方式 | 使用 dynamic client / unstructured，不引入 Istio SDK |
| 持久化策略 | 创建或更新时将 `auto` 解析为确定 provider 后持久化，避免环境变化导致行为漂移 |

## 4. 影响面分析

### 4.1 API 层

- `CreateApplicationsRequest`、`TryApplication`、应用详情、组件详情等复用 `spec.Traits` 的接口会自然暴露 `gateway` 字段。
- 需要在 DTO 展示中增加 Gateway 视图，避免只显示旧 `ingresses`。
- `externalLinks` 需要优先从 `gateway` 生成；没有 gateway 时继续回退到 `ingress` 或 service。
- API 鉴权、CORS、rate limit 路由不需要改变。

### 4.2 Domain 层

- `pkg/apiserver/domain/spec/traits.go` 新增 `Gateway` 字段。
- 新增 gateway provider、route、backend、TLS 等结构体。
- validation 需要覆盖 gateway 本身，并复用已有 ingress host、backend service 引用、resource name 等校验。
- create/update/try/import/convert 中需要保证 provider 解析契约一致。

### 4.3 DB 层

- 无 schema 变更。
- `ApplicationComponent.Traits` 当前按 JSON 结构保存，新字段可随 JSON 一起持久化。
- 兼容风险来自旧数据中只有 `ingress` 字段；运行时继续读取并按旧路径执行。

### 4.4 Cache 层

- 无 cache key、TTL、序列化协议变更。
- 如果组件详情缓存包含 `traits` 或 assembled DTO，发布后需依赖既有失效路径刷新；本方案不新增 cache entity。

### 4.5 K8s / Workflow 层

- 新增 `GatewayProcessor` 生成 additional objects。
- `ingress` provider 生成 `networking.k8s.io/v1 Ingress`。
- `istio` provider 生成 `networking.istio.io/v1 VirtualService`。
- workflow job builder 需要识别 VirtualService unstructured 对象并生成独立部署任务。
- job controller 需要通过 dynamic client apply/delete VirtualService。
- cleanup 需要支持删除由 Gateway Trait 生成的 VirtualService。

## 5. 公开契约设计

### 5.1 Traits 新字段

文件：`pkg/apiserver/domain/spec/traits.go`

```go
type Traits struct {
    Gateway []GatewayTraitSpec `json:"gateway,omitempty"`
    Ingress []IngressTraitsSpec `json:"ingress,omitempty"`
    // existing fields...
}
```

`Ingress` 保留，不在首期删除。新增能力只放入 `Gateway`，旧字段进入冻结维护状态。

### 5.2 Gateway 类型

建议新增文件：`pkg/apiserver/domain/spec/gateway.go`

```go
type GatewayProvider string

const (
    GatewayProviderAuto    GatewayProvider = "auto"
    GatewayProviderIngress GatewayProvider = "ingress"
    GatewayProviderIstio   GatewayProvider = "istio"
)

type GatewayTraitSpec struct {
    Name            string              `json:"name,omitempty"`
    Namespace       string              `json:"namespace,omitempty"`
    Provider        GatewayProvider     `json:"provider,omitempty"`
    GatewayRefs     []string            `json:"gatewayRefs,omitempty"`
    Hosts           []string            `json:"hosts,omitempty"`
    Labels          map[string]string   `json:"labels,omitempty"`
    Annotations     map[string]string   `json:"annotations,omitempty"`
    DefaultPathType string              `json:"defaultPathType,omitempty"`
    TLS             []GatewayTLSConfig  `json:"tls,omitempty"`
    Routes          []GatewayRouteSpec  `json:"routes"`
}

type GatewayTLSConfig struct {
    SecretName string   `json:"secretName"`
    Hosts      []string `json:"hosts,omitempty"`
}

type GatewayRouteSpec struct {
    Host     string             `json:"host,omitempty"`
    Path     string             `json:"path,omitempty"`
    PathType string             `json:"pathType,omitempty"`
    Backend  GatewayBackendSpec `json:"backend"`
    Rewrite  *RewritePolicy     `json:"rewrite,omitempty"`
}

type GatewayBackendSpec struct {
    ServiceName string            `json:"serviceName"`
    ServicePort int32             `json:"servicePort,omitempty"`
    Weight      int32             `json:"weight,omitempty"`
    Headers     map[string]string `json:"headers,omitempty"`
}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `name` | 网关资源名。为空时按 component name + gateway 序号生成。 |
| `namespace` | 目标资源 namespace。为空时使用 component namespace，再退到默认 namespace。 |
| `provider` | `auto`、`ingress`、`istio`。为空等同 `auto`。创建/更新时应解析为确定 provider 后持久化。 |
| `gatewayRefs` | Istio provider 使用，映射到 VirtualService `spec.gateways`。Ingress provider 忽略。 |
| `hosts` | 网关级 host 列表。route 级 `host` 优先。 |
| `labels` | 资源标签。必须过滤/拒绝 Eruun 保留标签。 |
| `annotations` | provider 透传注解。Ingress provider 可复用现有 nginx rewrite 注解规则。 |
| `defaultPathType` | route 未设置 `pathType` 时使用。Ingress provider 兼容现有行为。 |
| `tls` | Ingress provider 映射到 Ingress TLS。Istio provider 首期不支持，需由预置 Istio Gateway 处理。 |
| `routes` | 路由数组，必须非空。 |

### 5.3 Gateway provider 解析

新增运行配置：

```go
type GatewayConfig struct {
    Provider      string   // auto|ingress|istio
    IstioGateways []string // default gateways for istio provider
}
```

建议 flags / env：

| Flag | Env | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--gateway-provider` | `ERUUN_GATEWAY_PROVIDER` | `auto` | 空 provider 的全局选择策略 |
| `--gateway-istio-gateways` | `ERUUN_GATEWAY_ISTIO_GATEWAYS` | 空 | Istio provider 默认 `gatewayRefs`，支持逗号分隔 |

解析顺序：

1. trait 显式 `provider=ingress|istio`：直接使用。
2. trait `provider=""|auto`：读取全局 `gateway-provider`。
3. 全局 `gateway-provider=ingress|istio`：直接使用。
4. 全局 `gateway-provider=auto`：探测集群能力。
5. 探测到 `virtualservices.networking.istio.io` 且能解析出非空 `gatewayRefs`：选择 `istio`。
6. 其他情况选择 `ingress`。

Fail-fast 规则：

- 显式 `provider=istio` 但没有 trait/global `gatewayRefs`：返回校验错误。
- 显式 `provider=istio` 但集群没有 VirtualService CRD：返回构建错误，不静默降级到 ingress。
- `provider=auto` 且没有 Istio 条件时选择 ingress，这是默认选择规则，不视为异常兜底。

### 5.4 Istio provider 契约

首期只生成 VirtualService：

```yaml
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: web-gateway
  namespace: default
spec:
  hosts:
    - app.example.com
  gateways:
    - istio-system/public-gateway
  http:
    - match:
        - uri:
            prefix: /
      route:
        - destination:
            host: web-svc
            port:
              number: 80
```

映射规则：

| Gateway field | VirtualService field |
| --- | --- |
| `hosts` + route `host` | `spec.hosts` |
| `gatewayRefs` | `spec.gateways` |
| `routes[].pathType=prefix` | `http[].match[].uri.prefix` |
| `routes[].pathType=exact` | `http[].match[].uri.exact` |
| `routes[].pathType=regex|implementationSpecific` | `http[].match[].uri.regex` |
| `routes[].backend.serviceName` | `http[].route[].destination.host` |
| `routes[].backend.servicePort` | `http[].route[].destination.port.number` |
| `routes[].rewrite.replacement` | `http[].rewrite.uri` |

约束：

- route/backend 仍保持单 backend。`weight` 首期仅保留字段，不启用流量切分。
- `headers` 首期不映射到 Istio match，避免提前定义不完整的高级匹配语义。
- `tls` 在 Istio provider 下校验失败，错误信息指向“在预置 Istio Gateway 中配置 TLS”。
- `gatewayRefs` 格式允许 `name` 或 `namespace/name`，透传到 VirtualService `spec.gateways`。

### 5.5 示例

用户创建应用时推荐使用 `traits.gateway`：

```json
{
  "name": "demo",
  "component": [
    {
      "name": "web",
      "type": "webservice",
      "namespace": "default",
      "image": "nginx:1.25",
      "properties": {
        "ports": [{ "port": 80 }]
      },
      "traits": {
        "service": [
          {
            "name": "web-svc",
            "type": "internal",
            "ports": [{ "port": 80, "targetPort": 80 }]
          }
        ],
        "gateway": [
          {
            "provider": "istio",
            "gatewayRefs": ["istio-system/public-gateway"],
            "hosts": ["demo.example.com"],
            "routes": [
              {
                "path": "/",
                "pathType": "prefix",
                "backend": {
                  "serviceName": "web-svc",
                  "servicePort": 80
                }
              }
            ]
          }
        ]
      }
    }
  ]
}
```

Ingress provider 等价表达：

```json
{
  "provider": "ingress",
  "hosts": ["demo.example.com"],
  "defaultPathType": "prefix",
  "tls": [
    {
      "secretName": "demo-tls",
      "hosts": ["demo.example.com"]
    }
  ],
  "routes": [
    {
      "path": "/",
      "backend": {
        "serviceName": "web-svc",
        "servicePort": 80
      }
    }
  ]
}
```

## 6. 实现方案

### 6.1 Spec 与配置

变更点：

1. 在 `spec.Traits` 增加 `Gateway []GatewayTraitSpec`。
2. 新增 `pkg/apiserver/domain/spec/gateway.go`。
3. 在 `config.Config` 新增 `Gateway GatewayConfig`。
4. 在 `Config.AddFlags` 中注册 `gateway-provider` 与 `gateway-istio-gateways`。
5. 在 `Config.Validate` 中校验 provider 枚举与 Istio gateways 格式。

注意：

- 现有 `IstioEnable bool` 字段当前没有有效使用路径。不要复用它表达 provider 策略；可以在后续清理 PR 中删除或迁移。
- `GatewayProviderAuto` 只作为输入/配置值；持久化后的 component traits 应保存确定 provider。

### 6.2 Provider resolver

新增组件建议放在 `pkg/apiserver/workflow/traits/gateway` 或 `pkg/apiserver/domain/service`，职责如下：

1. 接收 `context.Context`、`config.GatewayConfig`、kube discovery client、trait。
2. 返回确定 provider、补齐后的 `GatewayTraitSpec`。
3. 显式 provider 不做隐式降级。
4. `auto` 探测结果允许短 TTL 缓存，建议 30-60 秒。

探测逻辑：

- 查询 API resources 是否包含 `networking.istio.io/v1/virtualservices`。
- 有 CRD 且 `gatewayRefs` 可解析时选择 `istio`。
- 其他情况选择 `ingress`。

### 6.3 Validation

文件：`pkg/apiserver/domain/service/validation.go`

新增 `validateGatewayTrait`：

1. `routes` 必须非空。
2. `provider` 仅允许 `""|auto|ingress|istio`。
3. `name` 必须符合 Kubernetes resource name。
4. `hosts`、`tls.hosts`、`routes[].host` 复用 Ingress host 校验。
5. 多个非 external service trait 且 route backend 缺 `serviceName` 时，复用现有 ingress 的明确 service name 要求。
6. `provider=istio` 且存在 `tls` 时返回明确错误。
7. `provider=istio` 且 trait/global 都没有 `gatewayRefs` 时返回明确错误。

冲突规则：

- `gateway` 与 `ingress` 并存不报错。
- 运行时只处理 `gateway`，跳过同层 `ingress`。
- 记录 warning：该 component 同时定义 gateway 和 ingress，已使用 gateway。

### 6.4 Trait processor 与 adapters

文件：

- `pkg/apiserver/workflow/traits/processor.go`
- `pkg/apiserver/workflow/traits/ingress.go`
- 新增 `pkg/apiserver/workflow/traits/gateway/*.go`

处理顺序：

1. `GatewayProcessor` 注册在 `IngressProcessor` 之前。
2. `applyTraitsRecursive` 如果发现 `traits.Gateway` 非空，应跳过 `ingress` processor，保证 `gateway` 优先。
3. 旧 `IngressProcessor` 保留，用于处理没有 gateway 的旧应用。

Adapter 接口：

```go
type Adapter interface {
    Provider() spec.GatewayProvider
    Build(ctx context.Context, component *model.ApplicationComponent, gw spec.GatewayTraitSpec) ([]client.Object, error)
}
```

Ingress adapter：

- 将 `GatewayTraitSpec` 转为 `IngressTraitsSpec`。
- 复用 `applyIngressDefaults`、`BuildIngress`、rewrite annotation、service/port 默认解析逻辑。
- 生成对象仍为 `*networkingv1.Ingress`。

Istio adapter：

- 生成 `*unstructured.Unstructured`。
- GVK 固定为 `networking.istio.io/v1, Kind=VirtualService`。
- metadata name/namespace/labels/annotations 使用 gateway trait 与 component 默认值。
- 不生成 Gateway、DestinationRule、ServiceEntry。

### 6.5 Workflow job builder 与 controller

当前 `AdditionalObjects` 只特殊识别 PVC、Ingress、RBAC 等强类型对象。VirtualService 是 unstructured，需要补齐部署/清理任务。

变更点：

1. 新增资源常量 `ResourceVirtualService` 与 job type `JobDeployVirtualService`。
2. `CreateObjectJobsFromResult` 识别 GVK 为 `networking.istio.io/v1/VirtualService` 的 unstructured 对象。
3. 为 VirtualService 设置 namespace、Eruun 管理标签、share 标签、resource info。
4. 新增 `DeployVirtualServiceJobCtl`，通过 dynamic client server-side apply。
5. `initJobCtl` 增加 `JobDeployVirtualService` 分支。
6. `jobRuntime` 注入 `*rest.Config` 或 dynamic client builder，避免在 controller 中重复获取 kubeconfig。
7. cleanup 支持按标签批量删除和按 AdditionalObjects 删除 VirtualService。

Fail-fast 要求：

- dynamic client 初始化失败时直接返回错误。
- apply VirtualService 失败时任务失败，不降级生成 Ingress。
- 显式 `provider=istio` 缺 CRD 时任务失败，并给出缺失 CRD 信息。

### 6.6 Convert 链路

文件：

- `pkg/apiserver/domain/service/kube_convert.go`
- `pkg/apiserver/domain/service/kube_convert_traits.go`
- `pkg/apiserver/domain/service/kube_convert_pipeline.go`

行为：

1. Kubernetes Ingress -> `traits.gateway(provider=ingress)`。
2. Istio VirtualService -> `traits.gateway(provider=istio)`。
3. 旧 `traits.ingress` 不再作为 convert 推荐输出；如兼容调用方确有需要，可只在过渡期同步输出，但默认文档示例使用 gateway。
4. 组件归属继续使用当前服务名/组件名匹配策略。
5. 无法匹配组件时返回 warning，不中断整体转换。

VirtualService 解析范围：

- `spec.hosts`
- `spec.gateways`
- `spec.http[].match[].uri.prefix/exact/regex`
- `spec.http[].route[0].destination.host`
- `spec.http[].route[0].destination.port.number`
- `spec.http[].rewrite.uri`

多 route destination 暂不完整映射；如果发现多个 destination，选择第一个并返回 warning，后续版本再设计流量拆分。

### 6.7 Assembler 与 external links

文件：`pkg/apiserver/interfaces/api/assembler/v1/do2dto.go`

新增展示结构：

```go
type ComponentGatewayInfo struct {
    Name        string                      `json:"name"`
    Namespace   string                      `json:"namespace"`
    Provider    string                      `json:"provider"`
    GatewayRefs []string                    `json:"gatewayRefs,omitempty"`
    Hosts       []string                    `json:"hosts,omitempty"`
    Annotations map[string]string          `json:"annotations,omitempty"`
    TLS         []spec.GatewayTLSConfig     `json:"tls,omitempty"`
    Routes      []ComponentGatewayRouteInfo `json:"routes,omitempty"`
}
```

规则：

1. 组件 DTO 新增 `gateways` 字段。
2. `externalLinks` 优先从 `gateways` 生成。
3. link type：
   - `gateway-ingress`
   - `gateway-istio`
4. 没有 gateway 时沿用现有 ingress/service link。
5. 对不部署入口的 component type 保持现有过滤规则。

### 6.8 API 与文档同步

需要更新：

- `docs/create-and-exec-application-api.md`
- `docs/validation-api-guide.md`
- 相关 examples 中新增 gateway 示例（代码实现 PR 中处理）

本文档 PR 只描述实现方案，不修改 examples。

## 7. 迁移策略

阶段 1：双轨兼容

- 新应用推荐使用 `traits.gateway`。
- 旧应用继续使用 `traits.ingress`。
- `gateway` 与 `ingress` 并存时使用 `gateway`。

阶段 2：提示迁移

- 创建/更新接口遇到 `traits.ingress` 时可返回 warning 或在日志中提示。
- 文档和示例默认只写 `gateway`。

阶段 3：冻结旧写入

- `ingress` 保留读取和执行兼容。
- 新增网关高级能力只进入 `gateway`。

## 8. 测试计划

### 8.1 单元测试

`pkg/apiserver/domain/service/validation_test.go`：

- gateway routes 缺失。
- provider 非法值。
- provider=istio 缺 gatewayRefs。
- provider=istio 且配置 tls。
- 多 service 且 route backend 缺 serviceName。
- gateway 与 ingress 并存校验通过。

`pkg/apiserver/workflow/traits/...`：

- GatewayProcessor 处理 ingress provider。
- GatewayProcessor 处理 istio provider。
- gateway 存在时跳过 ingress processor。
- Ingress adapter 与旧 BuildIngress 产物等价。
- Istio adapter 生成 VirtualService GVK、hosts、gateways、http route。

`pkg/apiserver/event/workflow/...`：

- VirtualService additional object 生成 deploy job。
- VirtualService job 通过 dynamic client apply。
- cleanup 能删除 VirtualService。

`pkg/apiserver/domain/service/...`：

- Ingress YAML convert 为 gateway provider=ingress。
- VirtualService YAML convert 为 gateway provider=istio。
- 多 destination VirtualService 返回 warning。

`pkg/apiserver/interfaces/api/assembler/v1/...`：

- gateway DTO 输出。
- externalLinks 优先 gateway。
- 没有 gateway 时回退 ingress。

### 8.2 回归命令

```bash
go test ./pkg/apiserver/domain/service -run 'Gateway|Ingress|Convert|Validation'
go test ./pkg/apiserver/workflow/traits/...
go test ./pkg/apiserver/event/workflow/... -run 'Gateway|VirtualService|ObjectJobs|Cleanup'
go test ./pkg/apiserver/interfaces/api/assembler/v1 -run 'Gateway|Ingress|ExternalLinks'
go test ./...
```

### 8.3 手工验收

Ingress provider：

1. 创建带 `traits.gateway(provider=ingress)` 的应用。
2. 执行 workflow。
3. 确认生成 Ingress、Service、Deployment。
4. 查询 components，确认 `gateways` 与 `externalLinks` 正确。

Istio provider：

1. 在集群中预置 Istio Gateway，例如 `istio-system/public-gateway`。
2. 设置 `ERUUN_GATEWAY_PROVIDER=istio` 或在 trait 中显式 `provider=istio`。
3. 创建带 `gatewayRefs` 的应用。
4. 执行 workflow。
5. 确认生成 VirtualService，不生成 Istio Gateway。
6. 确认 `spec.gateways` 引用预置 Gateway。

兼容路径：

1. 使用旧 `traits.ingress` 应用。
2. 执行 workflow。
3. 确认生成的 Ingress 与当前行为一致。

## 9. 验收标准

1. 旧 `traits.ingress` 应用行为不变。
2. 新 `traits.gateway(provider=ingress)` 能生成等价 Ingress。
3. 新 `traits.gateway(provider=istio)` 能生成 VirtualService。
4. 显式 Istio 配置错误时 fail-fast，不静默降级。
5. `gateway` 与 `ingress` 并存时只生成 gateway 对应资源。
6. `/applications/convert` 能识别 Ingress 与 VirtualService。
7. 组件详情能展示 gateway routes 和 gateway external links。
8. 不引入 DB schema 变更。
9. 不引入 Istio SDK 依赖。

## 10. 风险与规避

| 风险 | 规避 |
| --- | --- |
| 自动探测导致行为不透明 | 创建/更新时解析为确定 provider 并持久化；日志记录选择原因 |
| 显式 Istio 缺 CRD 时被误以为会 fallback | 显式 provider 失败即失败，不降级 |
| `gateway` 与 `ingress` 双轨造成歧义 | 明确 gateway 优先，并记录 warning |
| VirtualService 是 unstructured，部署链路漏处理 | 新增专门 job type 与 dynamic client controller |
| TLS 语义混淆 | 文档和校验明确：Istio TLS 在预置 Gateway 配置 |
| provider 扩展过度设计 | 首期只定义 adapter 接口和两个 provider，不提前创建其他 provider 实体 |

## 11. 实施清单

建议按以下 PR 拆分：

1. 文档 PR：完善本文档，确认实现契约。
2. Spec + validation PR：新增 `traits.gateway` 类型、配置、校验。
3. Processor + adapters PR：实现 ingress/istio resource build。
4. Workflow job PR：支持 VirtualService deploy / cleanup。
5. Convert + assembler PR：补齐转换和展示。

如果选择单 PR 实现，必须至少包含：

- 代码变更。
- 单元测试。
- docs 同步。
- 手工验收记录。

## 12. PR 说明模板

```markdown
## What
- Add `traits.gateway` as a provider-based entrance abstraction.
- Support ingress and Istio VirtualService providers.
- Keep existing `traits.ingress` compatibility.

## Why
- Decouple user-facing route intent from concrete gateway implementations.
- Allow Istio-based clusters to use the same application spec shape.

## Validation
- go test ./pkg/apiserver/domain/service -run 'Gateway|Ingress|Convert|Validation'
- go test ./pkg/apiserver/workflow/traits/...
- go test ./pkg/apiserver/event/workflow/... -run 'Gateway|VirtualService|ObjectJobs|Cleanup'
- go test ./pkg/apiserver/interfaces/api/assembler/v1 -run 'Gateway|Ingress|ExternalLinks'
- go test ./...
```
