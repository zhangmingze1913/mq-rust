# Raft 消息日志复制设计（1.0 剩余硬项）

> 状态：3/4 broker Raft、自动路由、追平、持久化、重启、分区、延时、事务、批量 apply、有界快照、bincode 日志、applied 水位已通过（R175–R218）；4 节点重启一致；batch512 负结果（R219）；5 万受硬件限制。

---

## 1. 目标

把 CommitLog 从“主推送、从落盘、主等 ack”升级为 Raft 日志：

- leader 选举、任期、投票、日志匹配；
- 多数派提交后才向客户端返回 SEND_OK；
- 旧 leader 任期过期后无法提交；
- 新 leader 必须拥有已提交日志；
- 副本断线重连后按 nextIndex 追平。

## 2. 方案

OpenRaft per broker group：

```text
Broker Raft group
  ├─ node 1 broker-a
  ├─ node 2 broker-b
  └─ node 3 broker-c
```

- `--raft-listen`：Raft TCP 端口；
- `--raft-node-id` / `--raft-peers`；
- `RaftLogStore`：持久化 vote/term/log/snapshot；
- `MessageStateMachine`：
  - apply `StoredMessage` -> `BrokerStore.append_replicated()`；
  - snapshot = 各 queue 的 offset 水位 + 元数据；
- SEND：
  - 仅 leader 接受；
  - `client_write(MessageCommand)`；
  - wait committed 后返回；
- Pull：
  - 线性读可走 leader lease；
  - follower 只读需 ReadIndex（1.0 可只允许 leader）。

## 3. 与现有组件关系

- TC Raft：事务状态，不动；
- broker leader lease：保留，作为路由/只读开关；
- namesrv 路由：leader 变更后由新 leader 发布；
- 现有 `replica_listen/replica_of`：Raft 模式下停用。

## 4. 关键难点

1. `BrokerStore` 是同步内存结构，apply 必须幂等；
2. 快照与 CommitLog/CQ 的一致性；
3. 批量 SEND 的日志条目粒度（单条 vs 批）；
4. 旧数据迁移：现有 quorum 数据如何建立初始 Raft 日志；
5. 性能：Raft 两阶段提交会降低单机 TPS，需要 pipeline/批提交。

## 5. 预估

- 设计与日志存储：3–5 天；
- 状态机/快照/恢复：1 周；
- SEND/Pull 接入与路由：1 周；
- 故障/分区/回归：1 周。

约 3–4 周专职工作，不能在当前 4 线程宿主上边压测边完成。

## 6. 当前替代

- Raft 租约选主（R163/R164）；
- quorum 同步复制（`--sync-replicate --replica-quorum 2`）；
- 旧主 `read_only` fence；
- 自动路由切换。

该组合可作 RC 的高可用方案，但不满足“消息日志 Raft 复制”的严格定义。
