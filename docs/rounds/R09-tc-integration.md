# R09 · Broker ↔ TC 集成（事务状态外置）

> 日期：2026-11-24  
> 状态：**Broker 支持 TC 多地址（三节点 TCP Raft）**；TC 宕机时进入持久化 outbox，leader kill 后自动切到新 leader。

---

## 1. 结果

Broker：

```text
--tc-url http://127.0.0.1:19401,http://127.0.0.1:19402,http://127.0.0.1:19403
```

三节点 TC 集群（leader=3）：

```text
TX RESULT commit=1 rollback=0
TC: ...committed / ...rolled_back
```

kill leader（node3）后，node1 成为新 leader：

```text
node1 {"leader":1,"is_leader":true}
TX RESULT commit=1 rollback=0
TC node1: ...committed / ...rolled_back
```

Broker 的 TC 客户端按地址列表依次尝试，follower 返回 500 时自动切换到新 leader。

---

## 2. 集成点

| Broker 事件 | TC 调用 |
|---|---|
| 收到 `TRAN_MSG` 半消息 | `POST /tx/{id}/prepare` |
| `END_TRANSACTION commit` | `POST /tx/{id}/commit` |
| `END_TRANSACTION rollback` | `POST /tx/{id}/rollback` |

实现：
- `mq-broker/src/tc.rs`：多地址 HTTP 客户端；
- `BrokerConfig.tc: Option<Arc<TcOutbox>>`；
- TC 故障时只记录日志，不影响本地 Half/CommitLog；
- outbox journal：`P` pending / `D` done，Broker 重启重放。

---

## 3. TC 宕机 + Broker 重启补齐

```text
（TC 停止）
TX RESULT commit=1 rollback=0

outbox journal:
P prepare ... / P commit ... / P prepare ... / P rollback ...

（TC + Broker 重启）
D prepare ... / D commit ... / D prepare ... / D rollback ...
TC: committed / rolled_back
```

---

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       53 项通过
```

---

## 5. 已知限制

| 限制 | 计划 |
|---|---|
| outbox 已持久化（无 compaction） | ✅ |
| TC 三节点多进程 TCP + Broker 多地址 | ✅ R12 |
| Broker 仍以本地 Half 为准 | R12.2 TC 决策为准 |
| Raft log/状态已持久化 | ✅ R14 |
| 无 TC 鉴权/TLS | 后续 |
| 无 ledger/对账 | R12.2 |

---

## 6. 下一轮

1. Broker 由 TC 决策驱动。
2. TC/Raft 快照与日志压缩。
3. outbox compaction。
4. 对账/审计链路。
