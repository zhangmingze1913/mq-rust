# R57 · `/stats` 按 Topic 明细

> 日期：2026-11-25  
> 结果：`/stats` 增加 `retained_dropped` 与 `topics_detail`，可看每个 topic 的消息数/字节。

---

## 1. 验证

发送两个 topic 各 15 条：

```json
{
  "body_bytes": 630,
  "messages": 30,
  "queues": 16,
  "retained_dropped": 0,
  "topics": 2,
  "topics_detail": [
    {"body_bytes":315,"messages":15,"topic":"MQ_TSTAT_A"},
    {"body_bytes":315,"messages":15,"topic":"MQ_TSTAT_B"}
  ]
}
```

## 2. 实现

- `BrokerStore::topic_stats()`：遍历 topic/queue 统计；
- admin `/stats` 输出明细；
- `retained_dropped` 同步展示。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 4. 限制

- 仍为全量遍历，topic 很多时开销随规模增长；
- 无历史趋势/持久化；
- 5 万 TPS/严格 HA/容器/TLS 未完成。
