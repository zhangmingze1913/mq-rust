# R08 · 综合验收（集成回归）

> 日期：2026-11-24  
> 环境：Windows 10、i5-4210M、JDK 17.0.2、RocketMQ 4.9.8 客户端、localhost  
> 结果：**核心功能 13/13 通过**；TC 三节点 Raft 单测通过；性能与 HA 仍为已知缺口。

---

## 1. 验收矩阵

| 场景 | 工具 / 类 | 结果 |
|---|---|---|
| 普通发送/拉取 | `MqRustCompatSmoke` | ✅ 15/15 |
| Tag 过滤 | `MqRustTagSmoke` | ✅ 3/3 |
| 批量消息 | `MqRustBatchSmoke` | ✅ 5/5 |
| 失败重投 | `MqRustRetrySmoke` | ✅ `reconsume=1` |
| 多消费者 Rebalance | `MqRustRebalanceSmoke` | ✅ c1=5 / c2=5 |
| 事务 commit/rollback | `MqRustTxSmoke` | ✅ commit=1 / rollback=0 |
| 事务 check-back | `MqRustTxCheckBackSmoke` | ✅ checks=1 / visible=1 |
| 事务超时回滚 | `MqRustTxTimeoutSmoke` | ✅ visible=0 |
| 死信 DLQ | `MqRustDlqSmoke` | ✅ attempts=2 / dlq=1 |
| SQL92 过滤 | `MqRustSql92PushSmoke` | ✅ count=1（price=150） |
| 延时消息 | `MqRustDelaySmoke` | ✅ early=0 / late=1 |
| 顺序消息 | `MqRustOrderSmoke` | ✅ 50 条 ordered=true |
| 幂等生产 | `MqRustIdempotentSmoke` | ✅ count=1 |
| `mqadmin clusterList` | 官方工具 | ✅ exit 0 |
| `mqadmin topicList` | 官方工具 | ✅ exit 0 |
| `mqadmin consumerProgress` | 官方工具 | ✅ exit 0 |

---

## 2. 事务 / TC

```text
TX RESULT commit=1 rollback=0
TX CHECK RESULT checks=1 visible=1
TX TIMEOUT RESULT visible=0
```

TC（Rust 单测）：

```text
tests::single_node_prepare_commit ... ok
tests::status_wal_survives_restart ... ok
tests::three_nodes_elect_and_replicate ... ok
```

---

## 3. 性能基线

```text
1 KiB / 8 线程 / 20k：7,127 TPS，avg 1.12ms，Failed=0（dedup 16 分片）
1 KiB / 4 线程 / 10k：5,556 TPS，avg 0.71ms
```

目标 5 万+ TPS 未达成；瓶颈与优化项见 [R04](R04-perf.md)。

---

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       49 项通过
```

---

## 5. 未完成项（如实记录）

| 项目 | 状态 |
|---|---|
| 跨进程 TCP Raft / TC 接管 Half | ⬜ |
| Raft log 持久化 / 快照 | ⬜ |
| 5 万+ TPS | ⬜（当前 4,050） |
| Docker 容器化 | ⬜（宿主无 Docker/WSL，已有 Dockerfile/compose） |
| ConsumeQueue 独立持久化索引 | ⬜ |
| 幂等消费 Inbox | ⬜ |
| ACL / TLS / gRPC | ⬜ |

---

## 6. 结论

- 功能面：**普通/顺序/事务/延时/批量/死信 + SQL92 + mqadmin** 已形成可运行闭环。
- 事务面：Half WAL、超时、check-back、审计、人工 commit/rollback、TC 三节点 Raft 均已有实现与测试。
- 生产面：**尚未达到 1.0 验收标准**（性能、HA、容器化、安全）。
