# R132 · SDK 最终矩阵（12 类）

> 日期：2026-11-25  
> 结果：12 类 SDK/管理场景全部通过；SQL92 Pull 仍受首心跳时序限制。

---

## 1. 结果

| 场景 | 结果 |
|---|---|
| Push | 5/5 ok |
| Retry | attempts=2 ok |
| Rebalance | c1=10 c2=6 ok |
| TxTimeout | visible=0 |
| TxCheckBack | checks=1 visible=1 |
| Idempotent | count=1 |
| DLQ | dlq=1，`%DLQ%` |
| SQL92 Push | count=1 |
| StartDeliver | early=0 late=1 |
| OffsetProgress | consumed=5 |
| Group CRUD | create=true delete=false |
| Admin（R129） | clusterList/topicList/topicStatus |

## 2. 说明

- 在 R129 之后再次验证管理/轨迹改动未破坏 SDK；
- Trace 持久化开启（`--trace-path`）不影响收发；
- SQL92 Pull 仍建议等首次心跳后再调用。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       81 项通过
```

## 4. 限制

- 未跑 SQL92 Pull；
- 单 broker/单分片；
- 5 万 TPS/容器/严格 Raft 未完成。
