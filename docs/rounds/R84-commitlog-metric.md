# R84 · CommitLog 磁盘字节指标

> 日期：2026-11-25  
> 结果：`/stats` 与 `/metrics` 新增 CommitLog 磁盘字节数；15 条消息 = 3,120 字节。

---

## 1. 验证

```json
/stats:
{"body_bytes":315,"commitlog_bytes":3120,"messages":15,...}

/metrics:
mq_broker_commitlog_bytes 3120
```

## 2. 实现

| 项 | 说明 |
|---|---|
| `BrokerStore::commitlog_bytes` | 汇总各 shard `CommitLog::len` |
| `/stats` | `commitlog_bytes` |
| `/metrics` | gauge，5s 刷新 |
| 用途 | 观察 Retention 未回收磁盘的问题 |

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```

## 4. 限制

- 仅 CommitLog，不含 CQ/index/审计文件；
- 重启 replay 不回收；
- 仍无 CommitLog 分段/清理。
