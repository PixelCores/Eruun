# Kafka 分布式消息队列实现文档

> 状态：Implemented Reference。本文描述当前 Kafka Queue 实现与运行配置；实际服务端 flag 以 `go run ./cmd/main.go --help` 为准。

## 目录

- [1. 背景与目标](#1-背景与目标)
- [2. 架构设计](#2-架构设计)
- [3. 实现细节](#3-实现细节)
- [4. 配置说明](#4-配置说明)
- [5. 使用指南](#5-使用指南)
- [6. 与 Redis Streams 对比](#6-与-redis-streams-对比)
- [7. 注意事项](#7-注意事项)
- [8. 改动文件清单](#8-改动文件清单)

---

## 1. 背景与目标

### 1.1 背景

Eruun 工作流引擎固定使用外部消息队列，可选择两种后端：

- **RedisStreams**：Redis Streams 后端
- **KafkaQueue**：Apache Kafka 后端

随着业务规模扩大，部分场景需要更高的吞吐量和更强的分布式能力，因此引入 Apache Kafka 作为新的消息队列后端。

### 1.2 目标

- 实现 `Queue` 接口的 Kafka 后端，并在 Kafka offset 语义下保证“至少一次投递 + 不跳过未确认消息”
- 提供简洁通用的配置方式，不增加过多复杂性
- 利用 Kafka 原生 Consumer Group 和 Rebalance 机制实现高可用
- 更新相关文档，确保使用者能够顺利切换使用

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                     Queue Interface                          │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │ RedisStreams│  │ KafkaQueue  │  ← NEW                   │
│  └─────────────┘  └─────────────┘                           │
└─────────────────────────────────────────────────────────────┘
         │                  │                  │
         ▼                  ▼                  ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    Redis    │    │    Kafka    │
└─────────────┘    └─────────────┘
```

### 2.2 Kafka 特性映射

| Queue 接口方法 | Kafka 实现方式 |
|---------------|---------------|
| `EnsureGroup` | 创建/切换 kafka.Reader，并按队列角色派生 Consumer Group |
| `Enqueue` | kafka.Writer 写入消息到指定 Topic，返回 producer correlation ID |
| `ReadGroup` | kafka.Reader.FetchMessage 从 Consumer Group 读取 |
| `Ack` | 仅提交每个 partition 的连续已确认 offset 前缀 |
| `AutoClaim` | 基于本地 pending 账本返回 stale 未确认消息 |
| `Stats` | 先做 broker 连通性探测，再返回 Lag 与 Pending 统计 |
| `Close` | 关闭 Writer 和 Reader |

### 2.3 消息流转图

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  Dispatcher │ ──────> │    Kafka    │ ──────> │   Worker    │
│  (Producer) │  WRITE  │   Broker    │   READ  │  (Consumer) │
└─────────────┘         └─────────────┘         └─────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │  Consumer Group │
                     │  (Coordination) │
                     └─────────────────┘
```

---

## 3. 实现细节

### 3.1 KafkaQueue 结构体

```go
type KafkaQueue struct {
    cfg    KafkaConfig
    writer kafkaWriter

    mu     sync.RWMutex
    reader kafkaReader

    pendingMu       sync.Mutex
    pendingMessages map[string]*pendingMessage
    pendingByPartition map[int]map[int64]*pendingMessage
}
```

核心设计要点：

1. **延迟初始化 Reader**：Reader 在 `EnsureGroup` 调用时创建，支持灵活配置
2. **分区级 Pending 账本**：同时维护 messageID 索引和 partition-offset 索引
3. **连续提交策略**：只提交连续已确认的 offset 前缀，避免跨 gap 提交导致的消息跳过
4. **线程安全**：使用读写锁保护 Reader，互斥锁保护 Pending 消息

### 3.2 消息 ID 生成

```go
func (k *KafkaQueue) messageID(msg kafka.Message) string {
    return fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
}
```

消息 ID 由 `partition:offset` 组成，确保全局唯一性。

### 3.3 AutoClaim 处理策略

Kafka 没有 Redis Streams 的 `XAUTOCLAIM` 等价物。本实现采用“本地 pending 重领 + Kafka Rebalance 兜底”策略：

```go
func (k *KafkaQueue) AutoClaim(ctx context.Context, ...) ([]Message, error) {
    // 返回 idle >= minIdle 的未 ACK 消息
    // 返回后刷新投递时间，避免热循环重复领取
}
```

**工作原理**：
- **进程内重试**：通过本地 pending 账本重新领取 stale 消息
- **进程间恢复**：当消费者崩溃或离组时，Kafka Rebalance 会把未提交 offset 对应消息重新投递给其他消费者

### 3.4 客户端初始化

```go
// clients/kafka.go
func EnsureKafka(cfg KafkaConfig) (*kafka.Dialer, error) {
    // 单例模式，确保全局只有一个 Dialer
    // 验证 Broker 连接性
    // 缓存连接供健康检查使用
}
```

---

## 4. 配置说明

### 4.1 配置结构

```go
type MessagingConfig struct {
    Type          string   // redis|kafka
    ChannelPrefix string   // 消息通道/Topic 前缀
    
    // Redis 配置
    RedisStreamMaxLen int64
    
    // Kafka 配置
    KafkaBrokers         []string  // Broker 地址列表
    KafkaGroupID         string    // Consumer Group ID
    KafkaAutoOffsetReset string    // earliest|latest
    KafkaTopicPartitions int       // 自动创建 Topic 的分区数
    KafkaTopicReplicationFactor int // 自动创建 Topic 的副本数
}
```

### 4.2 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--msg-type` | 消息队列类型 | `redis` |
| `--msg-channel-prefix` | 消息通道前缀；为空时运行时使用 `eruun` | 空 |
| `--msg-kafka-brokers` | Kafka Broker 地址列表 | `localhost:9092` |
| `--msg-kafka-group-id` | Consumer Group ID | `eruun-workflow-workers` |
| `--msg-kafka-offset-reset` | Offset 重置策略 | `earliest` |
| `--msg-kafka-topic-partitions` | 自动创建 Topic 分区数 | `1` |
| `--msg-kafka-topic-replication-factor` | 自动创建 Topic 副本数 | `1` |

Kafka Broker、消费组和 Offset 策略的默认值由 `NewConfig()` 显式提供，并展示在 `--help` 中。配置优先级为命令行参数 > `ERUUN_` 环境变量 > 默认值；默认消息后端仍为 Redis，只有设置 `--msg-type=kafka` 才会启用 Kafka。`localhost:9092` 仅用于本地连接，集群部署时应通过 `--msg-kafka-brokers` / `ERUUN_MSG_KAFKA_BROKERS` 指定可达的 Broker 地址；显式清空列表仍会校验失败。

### 4.3 配置示例

```bash
# 使用 Kafka 作为消息队列后端
./eruun-server \
  --msg-type=kafka \
  --msg-kafka-brokers=kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --msg-kafka-group-id=eruun-workflow-workers \
  --msg-kafka-offset-reset=earliest \
  --msg-kafka-topic-partitions=3 \
  --msg-kafka-topic-replication-factor=2
```

---

## 5. 使用指南

### 5.1 前置要求

1. 部署并启动 Kafka 集群（建议 3 节点以上）
2. 确保应用能够访问 Kafka Broker

### 5.2 切换到 Kafka

1. 修改启动参数：

```bash
--msg-type=kafka \
--msg-kafka-brokers=your-kafka-broker:9092
```

2. 或修改配置文件：

```yaml
messaging:
  type: kafka
  channelPrefix: eruun
  kafkaBrokers:
    - kafka-0.kafka.svc:9092
    - kafka-1.kafka.svc:9092
    - kafka-2.kafka.svc:9092
  kafkaGroupID: eruun-workflow-workers
  kafkaAutoOffsetReset: earliest
  kafkaTopicPartitions: 3
  kafkaTopicReplicationFactor: 2
```

### 5.3 验证连接

启动后，检查日志中是否有以下信息：

```
I kafka dialer initialized, connected to broker: kafka-0:9092
I kafka reader initialized for topic=eruun.workflow.dispatch group=eruun-workflow-workers
```

运行期健康检查区分为两类：

- `/healthz`：仅表示进程存活。
- `/readyz`：在 `msg-type=kafka` 时会检查 broker 连通性、`dispatch/delay/result` 三个真实业务 Topic 的元数据，并对每个 Topic 执行一次最小化 produce/read smoke check。

说明：

- smoke check 使用真实业务 Topic，不额外引入专用 health topic。
- smoke check 写入探针后会按 Kafka 返回的 partition/offset 精确读回，不依赖 consumer group rebalance 完成时机。
- 探针消息会由 Kafka 队列层识别，并按业务 consumer group 的连续 offset 提交规则确认，不会越过更早未 ACK 的业务消息，也不会进入 workflow/delay/result 业务处理链路。

---

## 6. 与 Redis Streams 对比

| 特性 | Redis Streams | Kafka |
|------|---------------|-------|
| **消息持久化** | 内存 + RDB/AOF | 磁盘 |
| **吞吐量** | 中等 (~100K msg/s) | 高 (~1M msg/s) |
| **消费者恢复** | AutoClaim (主动) | 本地 AutoClaim + Rebalance |
| **部署复杂度** | 低 | 中等 |
| **适用场景** | 中小规模 | 大规模 |
| **延迟** | 低 | 中等 |
| **消息回溯** | 有限 | 支持 |

### 6.1 选择建议

- **选择 Redis Streams**：
  - 已有 Redis 基础设施
  - 任务量适中（< 10K/s）
  - 需要低延迟
  - 希望简化运维

- **选择 Kafka**：
  - 需要高吞吐量
  - 需要消息持久化和回溯
  - 已有 Kafka 基础设施
  - 大规模分布式部署

---

## 7. 注意事项

### 7.1 Topic 自动创建

服务启动时会在 Kafka 初始化阶段做 Topic 保障：

1. 先检查 `dispatch/delay/result` 三个目标 Topic 是否可读元数据；
2. 若 Topic 不存在，主动按配置创建；
3. 创建后再次校验可用性；
4. 若任何 Topic 校验/创建失败，`msg-type=kafka` 模式下会 fail-fast（终止启动）。

仍可提前手工创建 Topic（推荐生产环境显式管理）：

```bash
kafka-topics.sh --create \
  --topic eruun.workflow.dispatch \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server kafka:9092
```

### 7.2 分区数配置

建议 Topic 分区数 >= Worker 数量，以实现最佳负载均衡。

### 7.3 启动行为（Kafka 模式）

- 当前行为：Kafka Topic/Broker 初始化失败即启动失败（fail-fast），避免“服务启动成功但队列实际不可用”。

### 7.4 Consumer Group 协调

- `--msg-kafka-group-id` 作为基础 group
- dispatch 队列使用基础 group（兼容旧部署）
- delay/result 队列派生为 `base.delay` / `base.result`，避免三类队列互相触发不必要 rebalance
- Rebalance 期间可能有短暂的消息处理延迟

### 7.5 Offset 提交策略

本实现使用手动提交 Offset（`CommitInterval=0`），并且只提交连续已确认前缀：

- 先 ACK 高 offset 不会提前提交（防止跳过低 offset 未确认消息）
- 低 offset 补齐后，才会推进提交点
- Worker 在处理后、提交前崩溃时，消息可能被重新消费
- 应用层仍需保持幂等（至少一次语义）

### 7.6 网络分区处理

Kafka 在网络分区时的行为：

- Producer 写入会超时重试
- Consumer 会触发 Rebalance
- 建议配置合理的超时参数

---

## 8. 改动文件清单

### 8.1 新增文件

| 文件路径 | 说明 |
|----------|------|
| `pkg/apiserver/infrastructure/messaging/kafka.go` | KafkaQueue 实现 |
| `pkg/apiserver/infrastructure/messaging/kafka_test.go` | KafkaQueue 单元测试 |
| `pkg/apiserver/infrastructure/clients/kafka.go` | Kafka 客户端初始化 |
| `docs/kafka-queue-implementation.md` | 本文档 |

### 8.2 修改文件

| 文件路径 | 改动说明 |
|----------|----------|
| `pkg/apiserver/config/config.go` | 新增 Kafka 配置字段和验证逻辑 |
| `pkg/apiserver/server.go` | 新增 `buildKafkaQueue` 方法 |
| `docs/workflow-architecture-guide.md` | 更新队列实现列表、配置说明和推荐配置 |
| `go.mod` | 新增 `github.com/segmentio/kafka-go` 依赖 |

### 8.3 测试覆盖

```bash
# 运行 Kafka 相关测试
go test ./pkg/apiserver/infrastructure/messaging/... -v -run Kafka

# 运行可选集成回归（需要 Docker 或可用 Kafka Broker）
scripts/kafka-regression.sh

# 测试用例包括：
# - TestKafkaQueueEnsureGroupDerivationAndSwitch   # group 派生和 reader 重建
# - TestKafkaQueueAckCommitsContiguousOffsetsOnly  # 连续 ACK 提交语义
# - TestKafkaQueueAckCommitsPerPartitionIndependently # 分区隔离提交
# - TestKafkaQueueAutoClaimStaleMessagesAndRefreshIdle # stale 消息重领
# - TestKafkaQueueStatsConnectivityFailure          # 连通性探测失败
# - TestKafkaQueueStatsWithPendingAndReaderLag      # lag/pending 统计
# - TestKafkaIntegrationOutOfOrderAckDoesNotSkipLowerOffsets (integration)
# - TestKafkaIntegrationAutoClaimReturnsStalePending (integration)
```

### 8.4 依赖变更

```
+ github.com/segmentio/kafka-go v0.4.49
+ github.com/klauspost/compress v1.15.9  (间接)
+ github.com/pierrec/lz4/v4 v4.1.15     (间接)
```

---

*文档版本：1.1.0*
*创建日期：2026-03*
*作者：Eruun Team*
