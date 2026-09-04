[English](./README.md) | [简体中文](./README_zh.md)

# Eruun

面向 Agent、模型与 AI 工作负载的分布式运行时。

Eruun 正在从 Kubernetes 工作流运行时演进为开源的自托管 AI 工作负载运行时。当前实现已经具备声明式应用、持久化工作流、空间隔离和分布式 Kubernetes 调和基础；Agent 工具、评测、向量化和模型服务只有在文档索引标记为 Current 后，才属于已实现产品契约。

## 能力成熟度

| 阶段 | 范围 | 状态 |
| --- | --- | --- |
| Current | 应用与组件生命周期、Traits、StepByStep/DAG 工作流、API/controller/scheduler/worker 四角色、Redis 或 Kafka 派发、MySQL 执行租约、Kubernetes 调和、校验、状态、日志、文件、Shell、认证与空间隔离 | 已在 `main` 实现 |
| Next | 容器化 Agent、MCP/CLI 工具绑定、凭据与权限边界、审计证据、基于 Workflow 的 Agent 评测 | 演进方向，公共契约尚未冻结 |
| Later | 自托管模型服务、GPU 感知调度、向量化流水线、托管 AI Provider 和更广泛的云或多集群执行 | 探索阶段 |

路线图首先解决 Kubernetes 自托管 Agent 及其安全边界，不在当前文档中引入独立顶层 Agent 资源，也不承诺尚未实现的 API。完整能力分层和决策门禁见 [AI Runtime 愿景](docs/ai-runtime-vision.md)。

## 当前运行时

Eruun 当前提供：

- 由 Component、Trait 和 Workflow 组成的 OAM 风格应用模型。
- 应用生命周期、工作流执行、校验、状态、日志、文件、Shell、设置、认证、空间和 namespace 纳管 REST API。
- 基于 Redis Streams 或 Kafka 的 StepByStep、DAG 工作流执行。
- 独立的 `api`、`controller`、`scheduler`、`worker` 角色，以及数据库 lease/fencing。
- Workload、Service、存储、RBAC、Ingress、探针、Sidecar、Init 容器、Rollout 和共享资源的 Kubernetes 调和。
- Helm 与独立 Manifest 两种部署方式。

Eruun 只提供服务端运行时，不包含客户端命令行应用。

## 快速开始

前置条件：Go 1.25、GNU Make、`kubectl`、Helm，以及可访问的 Kubernetes 集群。

启动前必须按照 [账号配置示例](deploy/accounts.example.json) 提供账号 Secret。认证、个人与团队空间、HTTPS 接入及 Kubernetes 隔离要求见 [账号与空间文档](docs/account-auth-workspaces.md)。

未显式提供 MySQL、Redis 密码时，安装脚本会在本地生成随机值；生成值只进入受保护的临时文件和 Kubernetes Secret。

```bash
AUTH_CONFIG_FILE=/secure/eruun/accounts.json SKIP_CONFIRM=true INSTALL_MODE=helm \
  ./deploy/all_in_one_install_quickstart.sh

kubectl -n eruun-system port-forward svc/eruun 8000:8000

curl --fail http://127.0.0.1:8000/api/v1/healthz
curl --fail http://127.0.0.1:8000/api/v1/readyz
```

本地开发可先按照 [本地依赖说明](docs/local-docker-dependencies.md) 启动 MySQL、Redis 和 Kafka，再运行：

```bash
export ERUUN_DATASTORE_URL='eruun:__REPLACE_WITH_MYSQL_PASSWORD__@tcp(127.0.0.1:3306)/eruun?charset=utf8mb4&parseTime=true'
export ERUUN_CACHE_PASSWORD='__REPLACE_WITH_REDIS_PASSWORD__'
export ERUUN_AUTH_CONFIG_FILE='/secure/eruun/accounts.json'
go run ./cmd/main.go
```

服务端本地运行时默认监听 `127.0.0.1:8001`。只有确实需要从 localhost 之外访问本地进程时，才设置 `ERUUN_BIND_ADDR=0.0.0.0:8001`；Kubernetes 部署路径会显式把监听端口覆盖为 `8000`。

## 运行架构

同一服务端二进制通过 `--role` 或 `ERUUN_ROLE` 选择且只启动一种角色：

- `api`：HTTP 契约、认证授权、校验和任务创建。
- `controller`：观察 Kubernetes，并把运行状态投影回数据库。
- `scheduler`：认领 waiting Workflow Run，发布带执行身份的派发消息。
- `worker`：消费派发消息，执行 Workflow 和 Kubernetes Job。

MySQL 是 Workflow 状态与执行 ownership 的持久化事实源。Redis 用于缓存、应用变更锁、取消信号和默认 Redis Streams 消息传输。Kafka 可以替代 Redis Streams 承载 Workflow 消息，但不能替代 MySQL ownership 或 Redis 中的应用级协调。

## 文档

- [文档索引](docs/README.md)：带状态标记的导航与当前代码事实。
- [AI Runtime 愿景](docs/ai-runtime-vision.md)：目标能力分层与路线图，不是已实现契约。
- [架构总览](docs/架构文档.md)：当前组件、Trait、Workflow 与运行时边界。
- [Workflow 架构](docs/workflow-architecture-guide.md)：当前工作流执行模型。
- [分布式运行时](docs/enterprise-distributed-runtime-design.md)：当前角色、租约、fencing 与恢复。

## 开发

```bash
make build
make test
go test ./... -race -cover
go vet ./...
```

## 许可证

Eruun 使用 MIT License，详见 [LICENSE](LICENSE)。
