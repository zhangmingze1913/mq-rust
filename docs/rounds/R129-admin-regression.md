# R129 · 管理接口后全量回归（双分片）

> 日期：2026-11-25  
> 结果：R118–R128 管理改动后，SDK 7 类场景与 mqadmin 命令全部通过。

---

## 1. 配置

```text
--commitlog-shards 2
--commitlog-segment-bytes 64MiB
--cq-segment-entries 1000
queues=8
```

## 2. SDK

| 场景 | 结果 |
|---|---|
| Compat | 15/15 |
| Batch | 5/5 |
| Tag | 3/3 |
| Tx | commit=1 rollback=0 |
| Order | 50/50 ordered=true |
| Delay | EARLY=0 LATE=1 |
| Inbox | 3→1 |

## 3. mqadmin

```text
clusterList   ✅ broker-a / MQ_RUST_R2
topicList     ✅ MQ_R129_D/T/O/TC... + TBW102
topicStatus   ✅ 8 队列 min/max offset
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       80 项通过
```

## 5. 说明

- 管理请求码新增未影响消息路径；
- 双分片下顺序/事务/延时正常；
- 5 万 TPS/容器/严格 Raft 仍未完成。
