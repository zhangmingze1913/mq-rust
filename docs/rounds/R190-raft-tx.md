# R190 · 事务消息 Raft 复制（half/commit/rollback）

> 日期：2026-11-26  
> 结果：事务 SEND 先本地 prepare，再 Raft 复制 half；commit/rollback 先本地生效，再复制事务操作；Java 事务 smoke 通过。

---

## 1. 与 R189 的差异

R189 让客户端响应走 Raft 模式，Java 事务客户端不再发送 END_TRANSACTION。  
R190 保持**原本地响应路径**，只是在其后追加 Raft 复制：

```text
SEND(tx)
  -> store.prepare_half(msg)          # 本地，响应字段与原路径一致
  -> raft.write(tx_kind=half)         # 复制 half
  -> 返回 SendResult

END_TRANSACTION commit
  -> store.commit_half(tx_id,msg_id)  # 本地生效
  -> raft.write(tx_kind=commit)       # 复制提交
  -> SUCCESS
```

rollback 同理。

## 2. 验证

```text
TX SEND commit=COMMIT_MESSAGE txId=...
TX SEND rollback=ROLLBACK_MESSAGE txId=...
TX RESULT commit=1 rollback=0
/stats messages=1（仅 commit topic）
```

## 3. 语义

- leader 先本地提交，保证客户端响应与原路径一致；
- Raft half/commit 按日志顺序应用，follower 最终一致；
- leader 上 commit 命令再次 commit 为 no-op（half 已移除）；
- 事务状态仍由 TC 持久化。

## 4. 边界

- 单节点验证，未跑 3 副本事务；
- leader 在本地提交后、Raft commit 复制前崩溃，follower 可能延迟提交；
- 分段日志/快照未做。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
