# R194 · 事务提交改为 Raft-first

> 日期：2026-11-26  
> 结果：commit/rollback 先写 Raft，再查本地结果；避免 leader 本地已提交但 Raft 未复制的窗口。

---

## 1. 变更

之前的 R190：

```text
commit_half 本地 -> raft commit
```

leader 本地提交后、Raft 复制前崩溃，follower 可能缺提交。

现在：

```text
raft commit/rollback -> 本地查询/无需再提交
```

- commit：Raft applier 在 leader 上执行 commit_half，随后 `message_by_id` 取回消息；
- rollback：Raft applier 执行 rollback_half，本地 `rollback_half` 不再调用；
- 响应仍在 Raft 提交后返回。

## 2. 验证

```text
TX SEND commit=COMMIT_MESSAGE
TX SEND rollback=ROLLBACK_MESSAGE
TX RESULT commit=1 rollback=0
/stats messages=1
```

## 3. 语义

- Raft 提交是事务可见性的唯一提交点；
- leader 本地 apply 与 follower 同序；
- 避免“本地已提交、Raft 未提交”的不一致窗口。

## 4. 边界

- leader 在 Raft commit 返回后、客户端响应前崩溃：事务已提交，客户端可能重试，需幂等；
- half 仍是本地 prepare + Raft 复制；
- 未做 check-back 与 Raft 的完整交互。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
