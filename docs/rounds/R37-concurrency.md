# R37 · 并发一致性压力测试

> 日期：2026-11-25  
> 结果：8 线程 × 100 条并发 append，8 个 queue 各 100 条，偏移连续，CQ/索引一致。

---

## 1. 测试

```text
test store::tests::concurrent_appends_are_consistent ... ok
```

- `Arc<BrokerStore>` 8 线程；
- 每线程写 100 条到固定 queue；
- 共 800 条；
- 每个 queue 读回 100 条，`queue_offset = 0..99`。

## 2. 覆盖点

- 读优先 topic map；
- 分区队列锁；
- dedup 16 分片；
- CommitLog 分片；
- CQ/索引写入；
- append_count 计数。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       61 项通过
```

## 4. 结论

并发 append/read 未发现偏移错乱或丢失；  
性能/HA/容器/安全缺口见 R30/R34。
