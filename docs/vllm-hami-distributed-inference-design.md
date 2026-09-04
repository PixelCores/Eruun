# Eruun 自托管模型服务与 GPU 方向

> 状态：Draft / Proposal。本文描述 Eruun 支持 vLLM、GPU 共享和多节点模型服务需要具备的能力，不提供当前可执行的请求，不冻结公共字段，也不指定唯一分布式后端。

## 1. 当前可复用能力

`main` 已经能够：

- 通过 `POST /api/v1/applications/create-and-exec` 创建并执行 Application。
- 使用 `job` 准备数据，使用 `webservice` 运行长期服务。
- 通过 storage、env/envFrom、service、ingress、probes、targetWorkEnv、securityPolicy 和 RBAC Traits 组合容器工作负载。
- 把 `traits.resources.gpu` 映射为 `nvidia.com/gpu` request/limit。
- 用 Workflow、数据库 lease/fencing、日志和组件状态管理部署过程。

这些能力能支撑普通 GPU Pod，但还不能构成完整的模型服务产品契约。

## 2. 当前缺口

当前公共声明不能稳定表达：

- 主容器和 init container 的独立 `args`、工作目录及模型服务启动参数。
- 任意 extended resources、Pod annotations、tolerations、affinity、topology spread 和更丰富的 GPU placement。
- HAMi 的显存、核心和调度参数。
- RayService、LeaderWorkerSet 或其他 CRD 的创建、Ready、升级、失败和清理语义。
- 模型 revision、下载完整性、缓存复用、制品清单和发布协议。
- 模型端点的语义健康检查、流量切换、弹性策略和服务级指标。

实现前必须先补通用 workload 表达与 operator adapter，不能通过在示例中填入当前 JSON 不接受的字段来假装可用。

## 3. 支持范围

目标分为两个逐步验证的 Profile：

### Profile A：单 Pod、单节点

- 一个不可变模型 revision。
- 一个准备模型制品的步骤，或引用已验证的只读制品。
- 一个使用单 GPU 或同节点多 GPU 的模型服务 Pod。
- 一个 ClusterIP Service，以及可选的受控 Ingress/Gateway。
- readiness 除端口存活外，还要验证模型可以完成最小推理请求。

Profile A 应先复用原生 Kubernetes workload，证明参数、GPU、存储、健康、更新和清理闭环。

### Profile B：多 Pod 或多节点

只有单节点闭环和真实模型规模证明需要后，才进入 Profile B。可选实现包括 vLLM 自身支持的分布式执行方式、Ray/Ray Serve、KubeRay、LeaderWorkerSet 或其他 operator-backed workload。

Eruun 的公共能力应表达：

- 节点/副本角色和期望拓扑。
- GPU、网络、共享内存、存储和 placement 要求。
- CRD 能力发现、创建、Ready、更新、失败、取消和清理。
- 服务端点及语义健康。

它不应把 Ray rank、vLLM 参数或模型层变成数据库实体。选择哪种 adapter 必须有版本兼容矩阵和集群验收，而不是由本文永久固定。

## 4. 模型制品

模型 ID 或可变 tag 不足以保证重复部署。每次部署应绑定不可变 revision，并记录来源、下载器版本、文件清单、大小和校验摘要。

建议的发布语义：

1. 下载到临时目录。
2. 校验完整性和必要文件。
3. 写入版本化 manifest。
4. 以目标存储支持的原子方式发布不可变目录。
5. 服务只挂载已完成且 manifest 匹配的 revision。

RWO、RWX、ROX 或每节点缓存是存储实现选择。Kubernetes access mode 不能证明跨节点文件锁、原子 rename 或一致性语义；使用共享存储前必须进行平台验收。Eruun 不应在共享 claim 缺失时静默创建不兼容的默认卷。

## 5. GPU 与 HAMi

当前 Eruun 只提供 `nvidia.com/gpu` 的固定映射。HAMi 等设备调度器可以通过 extended resources 和 annotations 暴露额外能力，但这些名称属于目标集群安装的版本化契约。

截至 2026-09-04 核验的 HAMi v2.5.1 文档包含 `nvidia.com/gpumem`、`nvidia.com/gpumem-percentage` 和 `nvidia.com/gpucores` 等配置。它们只能作为 adapter 示例：

- Eruun 公共设计应先支持受策略约束的扩展资源，而不是为每个插件增加字段。
- Validation/Try 需要区分静态声明错误和集群能力缺失。
- 资源回显必须展示最终 Kubernetes resource requests/limits，便于解释调度失败。
- GPU 共享、独占、优先级和抢占语义必须由平台能力验证，不能仅凭资源名推断。

参考：[HAMi configuration v2.5.1](https://project-hami.io/docs/v2.5.1/userguide/configure)。

## 6. vLLM 与分布式后端

vLLM 的 CLI 和 serving 形态持续演进。当前官方 `serve` 文档同时包含 `mp`、`ray` 等分布式执行后端和多节点参数；Ray Serve LLM 也是扩缩、负载均衡和多节点服务的一种选择。因此 Eruun 不把 RayService 写成唯一正确路径。

实现时需要锁定并测试：

- vLLM 镜像 digest 和支持的模型架构。
- CUDA/驱动、GPU 插件和通信库兼容关系。
- 单节点与多节点启动参数及 backend。
- 节点 IP、端口、DNS、NCCL/其他通信网络和共享内存。
- 服务 readiness、流式响应、取消、升级和故障诊断。

参考：[vLLM serve CLI](https://docs.vllm.ai/en/latest/cli/serve/)与[vLLM online serving](https://docs.vllm.ai/en/stable/serving/online_serving/)。实施 PR 必须记录实际核验版本，不能只依赖 `latest` 链接。

## 7. 安全边界

- 模型仓库 Token、对象存储凭据和服务 API key 只能通过引用式 Secret 或外部凭据机制注入。
- 不在 Application、Workflow、JobInfo、日志、trace 或指标 label 中保存明文凭据。
- 模型下载器与 serving 容器可以使用不同的 ServiceAccount、Secret 和出站网络策略。
- 对外暴露模型端点需要认证、速率限制、请求大小限制和访问日志策略；创建 Service 并不自动满足这些要求。
- operator CRD 的权限只授予负责该 adapter 的执行身份，不能扩大所有 Worker 的默认权限。

## 8. 可观测性与故障定位

最低观测面应覆盖：

- 制品下载、校验和缓存命中。
- Pod/CRD 调度、Pending 原因、GPU 分配和节点拓扑。
- 模型加载、readiness、请求延迟、吞吐、排队、Token 用量和错误。
- 多节点成员、通信错误和集体操作超时。
- Workflow task、模型 revision、workload 和服务端点之间的稳定关联。

高基数字段进入结构化日志或 trace，不默认作为 Prometheus label。

## 9. 更新、取消和清理

- 模型 revision 变化应创建可观察的新部署代次，旧实例在新实例语义就绪前不应被误判成功。
- 取消先停止后续步骤和流量切换，再按资源 ownership 清理本次创建对象。
- 失败清理不能删除预置模型卷、共享缓存或仅观察到的 operator 资源。
- 跨节点 workload 必须定义部分成员启动、operator 不可用、控制面重启和删除卡住时的终态。

具体 rollout、超时和保留默认值由实现与测试决定，不在本文固定。

## 10. 实施顺序与验收

1. 补齐通用容器参数和受控 extended resources，验证 API round-trip 与 Kubernetes 渲染。
2. 用原生 workload 完成 Profile A，包括不可变制品、GPU、Service 和语义 readiness。
3. 对目标集群做 HAMi 或其他设备插件能力探测和真实 GPU 验收。
4. 选择一个多节点 backend/adapter，完成 CRD 生命周期和版本矩阵。
5. 最后增加扩缩、流量切换和容量调度。

Profile A 升级为 Current 的最低标准是可执行示例、固定镜像/模型 revision、真实 GPU 集群测试、失败与取消验证、安全检查和运维说明。Profile B 必须另有多节点故障与恢复证据。
