# R07 · TC（Transaction Coordinator）Raft：单节点 + 三节点

> 日期：2026-11-24  
> 状态：**openraft 0.10；单节点、三节点进程内、三节点 TCP 选主/复制、状态 WAL 均通过**。多进程 CLI、Raft log 持久化、快照待补。

---

## 1. 结果

```text
POST /tx/tx-100/prepare -> {"tx_id":"tx-100","status":"prepared"}
GET  /tx/tx-100         -> {"tx_id":"tx-100","status":"prepared"}
POST /tx/tx-100/commit  -> {"tx_id":"tx-100","status":"committed"}
GET  /tx/tx-100         -> {"tx_id":"tx-100","status":"committed"}
```

Raft 单测：

```text
test tests::single_node_prepare_commit ... ok
test tests::status_wal_survives_restart ... ok
test tests::three_nodes_elect_and_replicate ... ok
test tests::tcp_three_nodes_elect_and_replicate ... ok
```

TCP 测试：
- 节点 1/2/3 分别监听 `127.0.0.1:19201/19202/19203`；
- 通过真实 TCP socket 发送 JSON RPC（AppendEntries/Vote）；
- 选出唯一 leader，prepare/commit 成功。

三节点测试：
- 节点 1/2/3 同时启动，进程内 registry 互相发送 AppendEntries/Vote；
- 选出唯一 leader；
- leader 上 `prepare` + `commit` 成功，状态为 `committed`。

---

## 2. 架构

新增 crate `mq-tc`：

- `openraft 0.10` + `openraft-memstore`；
- `Raft<TypeConfig, Arc<MemStateMachine>>`；
- 单节点 `node_id=1`，`initialize({1: ()})`；
- `ClientRequest.status` 存交易状态 `prepared/committed/rolled_back`；
- HTTP 服务 `127.0.0.1:9191` 提供 prepare/commit/rollback/status。

当前网络层是**进程内 registry**（`NetworkFactory`/`NetworkClient`），三节点测试已通过；生产需要替换为 TCP/HTTP 传输。

---

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       50 项通过
```

---

## 4. 已知限制

| 限制 | 计划 |
|---|---|
| 三节点 TCP 仅在单进程测试；多进程 CLI 未接 | R7.3 多进程启动参数 |
| Raft log 仍为 MemStore | R7.3 持久化 LogStore/StateMachine |
| 状态 WAL 已持久化 | ✅ |
| 未与 Broker 半消息集成 | ✅ R9 已完成 |
| 无快照 | R7.3 snapshot |

---

## 5. 下一轮 R7.1

1. 跨进程 TCP `RaftNetworkV2`。
2. 持久化 `RaftLogStorage`/`RaftStateMachine`。
3. 成员变更与快照。
4. Broker 通过 TC 写入 prepare/commit/rollback。
5. 故障注入：kill leader。
