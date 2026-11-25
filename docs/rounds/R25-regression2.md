# R25 · 补充回归与分片实验

> 日期：2026-11-25  
> 目的：补齐 SQL92 / StartDeliver / 人工事务 / Inbox SDK 回归，并验证 4 CommitLog 分片。

---

## 1. 回归结果

| 场景 | 结果 |
|---|---|
| SQL92 Push | `SQL92 PUSH RESULT count=1` |
| StartDeliver | `START RESULT early=0 late=1` |
| 人工 commit | `MANUAL POST ... -> 200`、`visible=1` |
| Inbox SDK | `delivered=3 processed=1` |

## 2. 4 分片性能

```text
8 线程 / 20k / 1 KiB：5,228 TPS
```

低于单分片 6.0–6.5k，保持默认 `--commitlog-shards 1`。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       59 项通过
```

## 4. 结论

SQL92、任意投递时间、人工事务、Inbox 均通过；  
4 分片在 HDD 上为负结果；5 万 TPS 仍未达成。
