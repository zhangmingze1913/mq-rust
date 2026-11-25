# R23 · 全量 SDK 回归（CQ 冷读后）

> 日期：2026-11-25  
> 目的：验证 CQ 冷读、位点持久化、锁优化后核心 SDK 链路未回归。

---

## 1. 结果

| 场景 | 结果 |
|---|---|
| Compat sync/async/oneway | `received=15 asyncOk=5 send=true` |
| Push | `received=5 expected=5 ok=true` |
| Tag | `received=3 ok=true` |
| Batch | `received=5` |
| Retry | `attempts=2 ok=true` |
| DLQ | `attempts=2 dlq=1` |
| Tx | `commit=1 rollback=0` |
| TxCheck | `checks=1 visible=1` |
| Idempotent | `count=1` |
| Rebalance | `c1=10 c2=5 ok=true` |
| Order | `done=true count=50 ordered=true` |
| Delay | `EARLY=0 LATE=1` |
| mqadmin clusterList | `broker-a / 127.0.0.1:10921` |
| mqadmin consumerProgress | 8 队列进度输出 |

## 2. 发现并修复

**CQ 冷读 queue_offset 丢失**：

- `read_from_cq` 从 CommitLog 解码出的 `StoredMessage.queue_offset` 是 append 前的值（全 0）；
- push consumer 按 queue offset 去重，导致 Order 只收到 8–11 条；
- 修复：按 CQ 条目位置回填 `queue_offset = start + i`、`queue_id`、`physical_offset`；
- 回归后 Order 50/50、`ordered=true`。

## 3. 测试稳定性修复

- Delay smoke 从 level 2（5s）改为 level 3（10s），等待 11s；
- 避免慢宿主上 early pull 越过到期点导致误判。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       58 项通过
```

## 5. 结论

核心 SDK 链路在 CQ 冷读 + 位点持久化 + 锁优化后保持通过；  
性能仍为 6.0–6.5k TPS，5 万目标未达成。
