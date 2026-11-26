# R191 · 三副本事务消息一致性

> 日期：2026-11-26  
> 结果：三 broker Raft 下事务 commit/rollback，三个副本均只有 1 条 commit 消息。

---

## 1. 步骤

```text
3 broker Raft + 自动路由
MqRustTxSmoke
```

## 2. 结果

```text
TX SEND commit=COMMIT_MESSAGE
TX SEND rollback=ROLLBACK_MESSAGE
TX RESULT commit=1 rollback=0

A/B/C /stats messages=1
topic = MQ_R8TX_COMMIT
```

三个副本都只有 commit topic 的 1 条消息，rollback topic 无消息。

## 3. 说明

- half 先本地 prepare，再 Raft 复制；
- commit/rollback 先本地生效，再复制事务操作；
- follower 按日志顺序 half -> commit/rollback；
- 事务状态仍由 TC 持久化。

## 4. 边界

- 未做 leader 崩溃时事务恢复；
- 未做 check-back 与 Raft 的交互；
- 分段日志/快照未做。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
