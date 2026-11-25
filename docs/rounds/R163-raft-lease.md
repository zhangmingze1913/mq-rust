# R163 · Raft 领导者租约（严格选主）

> 日期：2026-11-26  
> 结果：TC Raft 提供排他 broker 租约；双 broker 实测仅一个可写，TC leader 故障后自动切换。

---

## 1. 设计

- TC 已有 openraft 3 节点；
- 新增 `POST/GET /lease?candidate=<broker>&ttl_ms=<ms>`：
  - 仅 **TC leader** 处理；
  - 用 `tokio::Mutex` 串行化授予；
  - 当前 holder 未过期且为他人时拒绝；
  - 授予时通过 Raft `client_write` 写 `__lease__ = holder|term|expiry`。
- Broker：
  - `--leader-lease` 开启；
  - `--broker-id broker-a|broker-b`；
  - `--lease-ttl-ms 5000`；
  - 每 TTL/2 轮询三个 TC URL，只有 `holder == self` 时 `read_only=false`；
  - 连续 3 次失败置 `read_only=true`。

## 2. 双 broker 验证

```text
TC leader: node 2

broker-a (10921): read_only=false
broker-b (10931): read_only=true
```

仅一个 broker 可写。

## 3. TC leader 故障

kill TC node 2：

```text
node1 -> leader=1
broker-a: read_only=false
broker-b: read_only=true
```

旧 holder 在 TTL 内被新 leader 接管，租约重新授予后仍只有一个可写。

## 4. 边界

- 这是 **Raft 控制面选主 + 租约**，消息日志仍是 quorum 复制，不是 Raft 日志复制；
- namesrv 静态路由仍需外部/运维指向新主；
- 旧主 fence 仍由 `read_only` 保证；
- 未做 3 broker 网络分区矩阵。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
