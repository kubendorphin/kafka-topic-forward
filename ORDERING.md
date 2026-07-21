# Kafka 消息顺序保证方案

## 当前行为

当前实现使用 `LeastBytes` 负载均衡器，**不保证全局顺序**，但保证：
- ✅ 相同 Key 的消息顺序
- ✅ 分区内顺序

## 方案对比

### 方案 1: 保持当前实现（推荐）
**适用场景：** 大部分业务场景

```yaml
# 无需修改配置
```

**优点：**
- ✅ 最高吞吐量
- ✅ 负载均衡
- ✅ 相同 Key 顺序保证（适合大部分业务）

**缺点：**
- ❌ 无 Key 消息顺序不保证
- ❌ 全局顺序不保证

---

### 方案 2: 单分区写入（严格全局顺序）
**适用场景：** 必须保证全局顺序，且吞吐量要求不高

**代码修改：** forwarder.go

```go
writer := &kafka.Writer{
    Addr:     kafka.TCP(cfg.Dest.Brokers...),
    Topic:    cfg.Dest.Topic,
    // Balancer: &kafka.LeastBytes{},  // 删除这行
    Transport: &kafka.Transport{
        TLS:  destDialer.TLS,
        SASL: destDialer.SASLMechanism,
    },
    // ... 其他配置不变
}
```

**或者明确指定：**

```go
writer := kafka.NewWriter(kafka.WriterConfig{
    Brokers:   cfg.Dest.Brokers,
    Topic:     cfg.Dest.Topic,
    BatchSize: cfg.Forward.BatchSize,
    // 不设置 Balancer，使用默认 RoundRobin
    // 但需要目标 topic 只有 1 个分区
})
```

**优点：**
- ✅ 严格全局顺序

**缺点：**
- ❌ 吞吐量受限（单分区瓶颈）
- ❌ 无法水平扩展
- ⚠️ 需要目标 topic 只有 1 个分区

---

### 方案 3: 保留分区映射（推荐生产环境）
**适用场景：** 需要保持源分区顺序，且源/目标分区数相同

**代码修改：** forwarder.go

```go
// 在 FetchMessage 时记录原分区
m, err := f.reader.FetchMessage(ctx)
if err != nil {
    // ... 错误处理
}

// 加入批次时保留分区信息
batch = append(batch, kafka.Message{
    Key:       m.Key,
    Value:     m.Value,
    Headers:   m.Headers,
    Partition: m.Partition,  // ✅ 保留原分区
})
```

**配置要求：**
```yaml
# 确保源和目标 topic 分区数相同
```

**优点：**
- ✅ 保持源分区顺序
- ✅ 高吞吐量
- ✅ 可水平扩展

**缺点：**
- ⚠️ 需要源/目标分区数相同
- ⚠️ 如果目标分区数不同会写入失败

---

## 推荐方案

| 场景 | 推荐方案 | 理由 |
|------|---------|------|
| 日志、监控数据 | **方案 1** | 顺序不重要，吞吐量优先 |
| 订单、交易（有 Key） | **方案 1** | 相同 Key 顺序已保证 |
| 严格全局顺序 | **方案 2** | 牺牲性能换取顺序 |
| 生产级别迁移 | **方案 3** | 保持分区映射 |

---

## 验证顺序

### 检查消息是否有 Key
```bash
# 使用 kafka-console-consumer 查看
kafka-console-consumer --bootstrap-server source:9092 \
  --topic source-topic \
  --property print.key=true \
  --max-messages 10
```

### 检查分区数
```bash
kafka-topics --bootstrap-server source:9092 \
  --describe --topic source-topic

kafka-topics --bootstrap-server dest:9092 \
  --describe --topic dest-topic
```

---

## 当前实现总结

✅ **当前实现已经适合大部分场景**，因为：

1. **保留了原始 Key** → 相同 Key 的消息顺序不变
2. **批量写入** → 性能优异
3. **负载均衡** → 充分利用多分区

