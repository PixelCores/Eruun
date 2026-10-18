# 删除存量资源导入能力线与 Kafka 消息后端

## 背景

2026-10-18 的过度设计审计确认两块代码与当前产品定位不符：

- **存量资源导入 / adopted 接管**：源自 PaaS 时代的需求，在把已有 Kubernetes namespace 纳入管理时提供 `observe`（只读观察）和 `adopted`（经签名计划接管）两种模式。生产代码约 1.5 万行、测试约 1.7 万行，adopted 分支散布在几乎所有 Job 控制器、应用生命周期和版本更新路径中。Eruun 是面向 AI 工作负载的运行时，没有调用方依赖这条能力线，Console 也没有使用它。
- **Kafka 消息后端**：Helm 和静态清单都把后端写死为 Redis，而 Redis 本身又是认证、缓存、锁和取消信号的必需依赖。Kafka 实现需要在进程内存里模拟 Redis Streams 的 PEL/AutoClaim 语义，进程重启后 pending 状态会丢失，两者并不等价。

## 决策

两块一并删除，不保留兼容路径（Eruun 尚未发布）。

存量导入删除的范围：

- namespace 导入与 try 接口、resource-import 扫描/纳管 Job，以及对应的 gRPC RPC 与生成代码；
- `observe`/`adopted` 管理模式及 `managementMode` 字段、组件来源字段（source workload、resume replicas、加密 Secret 数据），以及 adoption snapshot；
- adopted 清理计划接口：`DELETE /applications/:appID/resources` 不再接受 `planFingerprint`，直接执行原生清理；
- importsecret keyring、`--import-secret-keyring*` 参数以及 Helm 的 `importSecretKeyring` 配置；
- adopted 专用的 PDB/NetworkPolicy Job 类型、Pod 协调器和管理模式迁移；
- `system_workspace` 授权类别与 `Scope.ClusterOperation`：它们只为导入放行跨 namespace 的集群级请求，删除后空间作用域内的 Kubernetes 请求一律经租户 transport 收敛。

Kafka 删除的范围：`--msg-type` 和 `--msg-kafka-*` 参数、Kafka 队列与客户端实现、本地 compose 中的 Kafka 服务，以及 dispatcher 中只为 Kafka 本地 pending 存在的处理生命周期标记。

## 契约变化

- 已移除的环境变量（`ERUUN_MSG_TYPE`、`ERUUN_MSG_KAFKA_*`、`ERUUN_IMPORT_SECRET_KEYRING*`）在启动时明确拒绝，与此前移除 `datastore-type`、`cache-type` 的处理方式一致。
- 写类 Job 在执行前仍会重新确认应用存在：原来的"管理模式校验"只保留了这一项正确性检查。
- 数据库里旧的 `management_mode`、`adoption_snapshot` 以及组件来源相关列不会被自动删除；GORM AutoMigrate 不删列，这些列会保持空置，也不再被读取。

## 未纳入

- 运行时 ClusterRole 中 `poddisruptionbudgets` 的权限在初始提交时就已存在，不属于导入能力线，本次没有调整。
- 历史审计文档与旧 devlog 保留原文，作为追溯记录。
