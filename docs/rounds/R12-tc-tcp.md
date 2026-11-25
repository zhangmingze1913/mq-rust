# R12 · TC 三节点 TCP Raft（多进程 + 故障切换 + 持久化）

> 日期：2026-11-25  
> 状态：**真实 TCP socket、三个独立 `mq-tc` 进程、kill leader 自动切换、follower 状态读取、状态 WAL 与 Raft log 持久化均通过**。

---

## 1. 结果

三进程启动：

```text
node1 {"node":1,"leader":2,"is_leader":false}
node2 {"node":2,"leader":2,"is_leader":true}
node3 {"node":3,"leader":2,"is_leader":false}
```

事务写入：

```text
POST /tx/tx-cluster/prepare -> prepared
POST /tx/tx-cluster/commit  -> committed
GET  /tx/tx-cluster         -> committed
```

kill leader（node2）后：

```text
node1 {"node":1,"leader":3,"is_leader":false}
node3 {"node":3,"leader":3,"is_leader":true}
POST /tx/tx-after-kill/commit -> committed
```

**follower 状态可见**：`StatusSM` 将 apply 的状态共享给 TC 服务，  
`node1/node2/node3` 都能查询到同一 tx 的 `committed`（测试断言）。

---

## 2. 进程与端口

| 节点 | Raft | API |
|---|---|---|
| 1 | 19301 | 19401 |
| 2 | 19302 | 19402 |
| 3 | 19303 | 19403 |

启动：

```powershell
mq-tc --id 1 --raft-listen 127.0.0.1:19301 --api-listen 127.0.0.1:19401 `
  --peers 1@127.0.0.1:19301,2@127.0.0.1:19302,3@127.0.0.1:19303
# 同理启动 2/3
```

持久化：

```powershell
mq-tc --id 1 --raft-listen 127.0.0.1:19301 --api-listen 127.0.0.1:19401 `
  --peers ... --data-dir G:/data/tc-node-1
```

---

## 3. 实现

`crates/mq-tc/src/tcp.rs`：

| 组件 | 说明 |
|---|---|
| `RpcRequest` / `RpcResponse` | serde JSON 枚举 |
| `NetworkFactory` | node id → `host:port` |
| `NetworkClient` | 4 字节长度 + JSON over TCP |
| `serve()` | 每连接一个 task，调用 `raft.append_entries/vote` |
| `Tc::start_tcp` | 绑定监听、创建 Raft、initialize |
| `Tc::start_tcp_wal` | 加状态 WAL |
| `Tc::start_tcp_persistent` | 加 `PersistentLogStore` |

`crates/mq-tc/src/log.rs`：JSON 文件保存 `vote/committed/log entries`，原子写。

Broker 集成：

```text
--tc-url http://127.0.0.1:19401,http://127.0.0.1:19402,http://127.0.0.1:19403
```

kill leader 后 Broker 的 TC 客户端自动切换到新 leader，事务仍 commit/rollback 成功。

### 持久化与 WAL 测试

```text
test tests::tcp_wal_survives_restart ... ok
test tests::persistent_log_survives_restart ... ok
test tests::three_node_persistent_restart ... ok
test tests::snapshot_contains_committed_status ... ok
```

- `StatusSM::apply` 写入 `tx_id\tstatus`；
- 单节点 `--data-dir` 重启：旧 tx 状态可读，新 tx 可提交；
- 三节点全部重启（同数据目录、新端口）后重新选主，旧状态可读、新 tx 可提交。
- `StatusSM` 实现 Raft 快照：`trigger().snapshot()` 生成的状态包含 `tx_id -> status`；快照写入 `node-<id>.snapshot.json`，重启后可恢复。
- 日志压缩已启用（`max_in_snapshot_log_to_keep=2`）：测试中 12 条日志压缩到 <12 条，重启后状态仍完整。

### 原生三节点脚本

```powershell
powershell -ExecutionPolicy Bypass -File deploy/native/start-tc-cluster.ps1
# 输出：node2 leader、node1/node3 follower
powershell -ExecutionPolicy Bypass -File deploy/native/stop-tc-cluster.ps1
```

实测：`tx-native-cluster=committed`，停止全部节点再启动后仍可查询到 `committed`。

---

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       54 项通过
```

---

## 5. 已知限制

| 限制 | 计划 |
|---|---|
| Raft log 持久化（`--data-dir`） | ✅ |
| 状态 WAL（TCP）已持久化 | ✅ |
| 三节点持久化重启 | ✅ |
| 原生三节点脚本 | ✅ `start-tc-cluster.ps1` |
| 快照持久化 + 日志压缩 | ✅ |
| 无鉴权/TLS | 后续 |
| Broker 仍以本地 Half 为准 | R12.2 |

---

## 6. 下一轮

1. 快照与日志压缩。
2. Broker 由 TC 决策驱动。
3. 三 TC 节点容器 compose。
4. 性能提升。
