# R98 · 最终功能回归（双 CommitLog 分片）

> 日期：2026-11-25  
> 结果：`--commitlog-shards 2` 下 Compat/Batch/Tag/Tx/Order/Delay/Inbox 全部通过。

---

## 1. 配置

```text
--commitlog-shards 2
--commitlog-segment-bytes 64MiB
--cq-segment-entries 1000
queues=8
```

## 2. 结果

| 场景 | 结果 |
|---|---|
| Compat | 15/15 |
| Batch | 5/5 |
| Tag | 3/3 |
| Tx | commit=1 rollback=0 |
| Order | 50/50 ordered=true |
| Delay | EARLY=0 LATE=1 |
| Inbox | 3→1 |

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 说明

- 覆盖 quorum 配置引入后的协议路径；
- 分片按 queue_id 取模，跨分片队列仍有序；
- 5 万 TPS、容器实测仍未完成。
