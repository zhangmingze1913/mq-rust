# R40 · Admin 存储统计 `/stats`

> 日期：2026-11-25  
> 结果：新增 `GET /stats`，返回 topic/queue/消息数/body 字节数。

---

## 1. API

```text
GET http://127.0.0.1:9091/stats
```

示例（15 条 21 字节消息）：

```json
{"body_bytes":315,"messages":15,"queues":8,"topics":1}
```

## 2. 实现

- `BrokerStore::stats()`：遍历 topic/queue 统计 messages、body bytes；
- admin `/stats` 输出 JSON；
- 无额外锁顺序风险（读锁内只读）。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       62 项通过
```

## 4. 用途

- 运维快速查看 Broker 内存消息量；
- 后续可接入 Prometheus gauge（当前为 HTTP 查询）。

## 5. Prometheus Gauges

`/metrics` 已增加：

```text
mq_broker_store_topics 1
mq_broker_store_queues 8
mq_broker_store_messages 15
mq_broker_store_body_bytes 315
```

后台每 5s 刷新，15 条消息验证一致。
