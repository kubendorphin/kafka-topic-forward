# kafka_topic_forward

将 Kafka 一个 topic 的所有数据转发到另一个 topic，源/目标信息通过配置文件读取，支持限速。

## 特性

- 配置文件驱动：源 kafka/topic、目标 kafka/topic 全部从 `config.yaml` 读取
- 支持跨集群转发（源和目标可以是不同的 kafka 集群）
- 保留消息的 Key 和 Headers
- **至少一次（at-least-once）语义**：先写入目标、再提交源端位点
- 限速：支持按「每秒消息条数」和「每秒字节数」限速（令牌桶）
- 优雅退出：收到 Ctrl+C / SIGTERM 后停止并打印统计

## 构建

```bash
go build -o ./bin/kafka_topic_forward.exe .
```

## 运行

```bash
./bin/kafka_topic_forward.exe -config config.yaml
```

## 配置说明（config.yaml）

| 字段 | 说明 |
| --- | --- |
| `source.brokers` | 源 kafka broker 列表 |
| `source.topic` | 源 topic |
| `source.group_id` | 消费组（不填默认 `kafka-topic-forward`），重启后从已提交位点继续 |
| `dest.brokers` | 目标 kafka broker 列表 |
| `dest.topic` | 目标 topic |
| `forward.start_from_earliest` | 首次消费时 `true`=从最早开始，`false`=只转发新数据（仅在该消费组无已提交位点时生效） |
| `forward.batch_size` / `batch_timeout` | 生产端批量参数 |
| `forward.commit_interval` | 位点提交间隔 |
| `forward.min_bytes` / `max_bytes` | 单次拉取字节范围 |
| `rate_limit.enabled` | 是否开启限速 |
| `rate_limit.messages_per_second` | 每秒转发条数上限，`<=0` 不限 |
| `rate_limit.bytes_per_second` | 每秒转发字节上限，`<=0` 不限 |
| `rate_limit.burst` | 令牌桶突发容量，`<=0` 自动取速率值 |

两种限速可同时开启，任一触发即等待。
