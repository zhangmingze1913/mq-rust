# R31 · 最终回归（R30 后全链路）

> 日期：2026-11-25  
> 目的：在 TC/CQ/Inbox/HA/元数据压缩全部落地后，做最后一次 SDK 回归。

---

## 1. 结果

| 场景 | 结果 |
|---|---|
| Compat sync/async/oneway | `received=15 asyncOk=5 send=true` |
| 事务 | `commit=1 rollback=0` |
| Inbox SDK | `delivered=3 processed=1` |
| Order | `done=true count=50 ordered=true` |
| Delay | `EARLY=0 LATE=1` |
| mqadmin clusterList | `DefaultCluster broker-a 127.0.0.1:10921` |
| mqadmin consumerProgress | queue0 offset=50 |

## 2. 环境

```text
broker: 1 CommitLog shard, 8 queues, metadata-compact-ms=5000
store:  G:/MQ_soft/mq-rust-data/final77
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
```

## 4. 结论

R30 后的所有核心链路无回归；性能基线保持 100k 长跑 **9,347 TPS**。  
项目仍未达到生产 1.0（5 万 TPS、容器实测、严格 HA、安全）。
