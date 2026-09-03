[English](./README.md) | [简体中文](./README_zh.md)

# Eruun

面向 Agent、模型与 AI 工作负载的分布式运行时。

Eruun 是一个开源 Kubernetes 应用运行时，以声明式组件、traits 和持久化工作流为核心。当前版本提供 API Server、分布式 controller/scheduler/worker 角色、Kubernetes 资源调和、生命周期工作流、校验、状态与运维 API。

Agent evaluation、向量化和分布式模型服务相关文档只有在文档索引中明确标记为 Current 时才代表已实现能力；标记为 Draft / Proposal 的内容不属于当前产品契约。

## 当前能力

- 基于组件、traits 和 workflow 的 OAM 风格应用模型。
- 应用生命周期、工作流执行、校验、状态、日志、文件、Shell、系统设置和 namespace 纳管 REST API。
- 基于 Redis Streams 或 Kafka 的 StepByStep、DAG 工作流执行。
- API、controller、scheduler、worker 四类运行角色，以及数据库 lease/fencing。
- 对 Workload、Service、存储、RBAC、Ingress、探针、Sidecar、Init 容器、Rollout 和共享资源的 Kubernetes 调和。
- Helm 与独立 Manifest 两种部署方式。

Eruun 只提供服务端运行时，不包含客户端命令行应用。

## 快速开始

启动前必须按照 [账号配置示例](deploy/accounts.example.json) 提供统一账号 Secret。GitHub/Google、邮箱/手机号登录、个人和团队空间、HTTPS 接入及 Kubernetes 隔离要求见 [账号与空间文档](docs/account-auth-workspaces.md)。集群须支持 Restricted v1.34 Pod Security 并实际执行 NetworkPolicy。

前置条件：Go 1.25、GNU Make、kubectl、Helm，以及可访问的 Kubernetes 集群。

未显式提供 MySQL、Redis 密码时，安装脚本会在本地生成随机值。生成值只进入受保护的临时文件和 Kubernetes Secret。

~~~bash
AUTH_CONFIG_FILE=/secure/eruun/accounts.json SKIP_CONFIRM=true INSTALL_MODE=helm \
  ./deploy/all_in_one_install_quickstart.sh

kubectl -n eruun-system port-forward svc/eruun 8000:8000

curl --fail http://127.0.0.1:8000/api/v1/healthz
curl --fail http://127.0.0.1:8000/api/v1/readyz
~~~

本地运行服务端：

~~~bash
export ERUUN_DATASTORE_URL='eruun:__REPLACE_WITH_MYSQL_PASSWORD__@tcp(127.0.0.1:3306)/eruun?charset=utf8mb4&parseTime=true'
export ERUUN_CACHE_PASSWORD='__REPLACE_WITH_REDIS_PASSWORD__'
export ERUUN_AUTH_CONFIG_FILE='/secure/eruun/accounts.json'
go run ./cmd/main.go
~~~

服务端默认监听 127.0.0.1:8000。只有确实需要从 localhost 之外访问时，才设置 ERUUN_BIND_ADDR=0.0.0.0:8000。

## 运行架构

Eruun 以四类显式运行角色之一启动；Helm 与独立 Manifest 会分别部署全部四类角色：

- api：HTTP 路由、请求校验和响应契约。
- controller：Kubernetes 观察与运行状态同步。
- scheduler：工作流调度与分发。
- worker：工作流 Job 与 Kubernetes 资源调和。

直接在本地运行服务端时默认使用 `api` 角色，不提供聚合的 `all` 角色。

Redis 用于缓存、工作流锁和默认 Redis Streams 消息传输。工作流消息可以切换为 Kafka，但缓存和锁仍需要 Redis。MySQL 是持久化状态主存储。

## 配置

每个服务端 flag 都有由 flag 名自动生成的 ERUUN_ 环境变量。常用配置包括：

- ERUUN_BIND_ADDR
- ERUUN_DATASTORE_URL
- ERUUN_CACHE_HOST、ERUUN_CACHE_PORT、ERUUN_CACHE_PASSWORD
- ERUUN_MSG_TYPE、ERUUN_MSG_KAFKA_BROKERS
- ERUUN_AUTH_CONFIG_FILE
- ERUUN_ROLE

完整服务端参数：

~~~bash
go run ./cmd/main.go --help
~~~

默认配置说明见 config/apiserver-default.yaml。

## 开发

~~~bash
make build
make test
go test ./... -race -cover
go vet ./...
~~~

主要文档：

- docs/README.md — 带状态标记的文档索引
- docs/workflow-architecture-guide.md — 工作流引擎架构
- docs/enterprise-distributed-runtime-design.md — 分布式角色与租约
- docs/core-module-boundary-and-cross-layer-contracts.md — API、领域、持久化和 Kubernetes 边界
- examples/ — HTTP 请求载荷与运维示例

## 许可证

Eruun 使用 MIT License，详见 LICENSE。
