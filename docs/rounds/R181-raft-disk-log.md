# R181 · Raft 日志持久化

> 日期：2026-11-26  
> 结果：新增 `DiskLogStore`；单节点重启后保留 Raft 日志/投票，继续 SEND，消息数 2。

---

## 1. 实现

`crates/mq-raft/src/disk.rs`：

- 内存 `MemLogStore` + JSON 快照文件 `raft.log`；
- 持久化：vote、purged、entries；
- 每次 append/truncate/purge/save_vote 原子重写；
- 启动时恢复 vote、entries、purged，再交给 openraft。

Broker：

```text
store_root/raft.log
```

## 2. 验证

```text
mqadmin send M1
SEND_OK
raft.log 630 bytes

kill broker
restart 同一 store
mqadmin send M2
SEND_OK

/stats messages=2
```

重启后节点重新选主并继续接收写入，旧日志恢复。

## 3. 边界

- 全量 JSON 重写，日志大时性能差；
- 未做分段/压缩；
- 状态机快照未持久化，重启会从日志重放；
- 3 节点 leader/全集群重启未测；
- 批量/延时/事务未走 Raft。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
