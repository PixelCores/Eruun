# 本地 Docker 开发依赖

> 状态：Current。仅用于本机开发，不是生产部署方案；不会启动 Eruun 服务端或 Kubernetes。

`deploy/compose.yaml` 把 MySQL、Redis、Kafka 放在同一个 `eruun-dev` Compose 分组中，Docker Desktop / OrbStack 可以整体启动、停止。它不会接管或删除已有的独立容器。

| 服务 | 默认镜像 | 宿主机地址 | Compose 网络地址 | 持久化目录 |
| --- | --- | --- | --- | --- |
| MySQL | `mysql:8.4.11` | `127.0.0.1:3306` | `mysql:3306` | `/var/lib/mysql` |
| Redis | `redis:7.2.16-alpine` | `127.0.0.1:6379` | `redis:6379` | `/data` |
| Kafka | `apache/kafka:3.9.1` | `localhost:9092` | `kafka:19092` | `/var/lib/kafka/data` |

三个服务各自使用命名卷，均有健康检查与资源限额/预留。宿主机端口只绑定 `127.0.0.1`。Kafka 是单节点 KRaft broker/controller，副本数为 1，不需要 ZooKeeper；它没有配置 SASL/TLS，只应供可信本地进程及同一 Compose 网络内的容器访问。配置依据见 [Apache Kafka 镜像说明](https://hub.docker.com/r/apache/kafka/)。

## 启动

需要运行中的 Docker Engine，以及支持环境变量来源 secrets、`up --wait` 和 `--wait-timeout` 的 Docker Compose。

在仓库根目录运行。下面的随机密码只在**首次初始化**时生成；请自行安全保存，后续启动必须继续使用同一组值，不要反复生成：

```bash
export MYSQL_ROOT_PASSWORD="$(openssl rand -hex 24)"
export MYSQL_PASSWORD="$(openssl rand -hex 24)"
export REDIS_PASSWORD="$(openssl rand -hex 24)"
./deploy/local_start_deps.sh
```

脚本要求这三个密码非空且不含 `__REPLACE_` 占位符，不会打印密码，也不写入仓库。Compose 从当前 shell 的环境变量读取密码并挂载为 secret 文件；不是固定默认密码，也不是从 YAML 明文读取。参见 [Docker Compose secrets](https://docs.docker.com/reference/compose-file/secrets/)。Docker 管理权限仍可读取容器中的 secret。

脚本最多等待 180 秒，全部服务健康后才报告成功；重复执行会复用同一分组和命名卷。失败时保留容器便于排查，不自动删除数据。MySQL 使用独立的 `eruun` 用户及 `eruun` 数据库；root 密码与应用密码分别设置。

如有端口冲突，可在首次启动前设置 `MYSQL_PORT`、`REDIS_PORT`、`KAFKA_PORT`。Kafka 对外公布的地址会同步使用 `KAFKA_PORT`。还可设置 `MYSQL_DATABASE`、`MYSQL_IMAGE`、`REDIS_IMAGE`、`KAFKA_IMAGE`；镜像覆盖应使用明确版本，不要使用 `latest`。`COMPOSE_PROJECT_NAME` 可覆盖分组名，所有后续 Compose 命令必须使用相同值。

旧脚本的 `MYSQL_CONTAINER_NAME` / `REDIS_CONTAINER_NAME` 不再使用，容器名由 Compose 分组生成。旧的 MySQL 数据卷不会自动迁移到本环境；尤其不要把 MySQL 8.0 的旧卷直接挂到这里默认的 8.4 镜像。

## 连接 Eruun

在同一 shell 中配置服务端；MySQL 使用上面生成的应用密码，不使用 root 密码：

```bash
export ERUUN_DATASTORE_URL="eruun:${MYSQL_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT:-3306})/${MYSQL_DATABASE:-eruun}?charset=utf8mb4&parseTime=true"
export ERUUN_DATASTORE_DATABASE="${MYSQL_DATABASE:-eruun}"
export ERUUN_CACHE_HOST=127.0.0.1
export ERUUN_CACHE_PORT="${REDIS_PORT:-6379}"
export ERUUN_CACHE_PASSWORD="${REDIS_PASSWORD}"
export ERUUN_MSG_TYPE=kafka
export ERUUN_MSG_KAFKA_BROKERS="localhost:${KAFKA_PORT:-9092}"
export ERUUN_AUTH_CONFIG_FILE=/secure/eruun/accounts.json
go run ./cmd/main.go
```

仍需准备 [账号配置](account-auth-workspaces.md) 和可访问的 Kubernetes。服务端默认只有 `api` 角色；完整工作流需要按 [运行架构](enterprise-distributed-runtime-design.md) 分别运行其他角色。

若使用默认 Redis Streams 队列，将 `ERUUN_MSG_TYPE` 设为 `redis`。启动 Kafka 容器不会自动切换 Eruun 的消息后端；使用 Kafka 时 Redis 仍用于缓存和应用锁。

## 查看、停止与数据保留

```bash
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml logs --tail=100
docker compose -f deploy/compose.yaml stop
```

再次运行启动脚本可以恢复服务。`docker compose -f deploy/compose.yaml down` 会移除该分组的容器和网络，但保留命名卷。不要加 `--volumes` / `-v`，除非明确要删除这套环境的全部数据库、Redis 和 Kafka 数据。

MySQL 的密码与数据库初始化变量只对空数据目录生效。修改环境变量不会更新已有数据库的密码；应使用原凭据或显式执行数据库用户密码变更。若密码与卷中账号不一致，MySQL 健康检查会失败。

## 验证

`go test ./deploy` 检查 Compose 契约与启动脚本的凭据校验、错误传播。真实 Docker 验收需显式启用：

```bash
ERUUN_TEST_LOCAL_DEPS=1 go test ./deploy -run '^TestLocalDependenciesIntegration$' -count=1 -timeout=15m
```

此测试拉取镜像、创建随机命名的独立 Compose 分组和空卷，验证宿主机 MySQL/Redis/Kafka 读写及停止再启动后的数据保留。测试结束时删除它自己创建的容器、网络和卷，不操作已有开发分组。
