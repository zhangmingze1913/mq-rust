# R189 · 事务消息 Raft 尝试（负结果，已回退）

> 日期：2026-11-26  
> 结果：half 已通过 Raft 复制，但 Java 事务客户端未发送 END_TRANSACTION；事务 smoke commit=0，代码已回退。

---

## 1. 尝试

- `MessageCommand` 增加 `tx_id/tx_kind`；
- SEND 事务消息写 `tx_kind=half`；
- END_TRANSACTION 写 `commit/rollback`；
- applier 调用 `prepare_half/commit_half/rollback_half`。

## 2. 现象

```text
raft tx apply tx_kind=Some("half") tx_id=... msg_id=...
10s 后 expired half transactions rolled back
TX RESULT commit=0 rollback=0
```

- half 已 apply；
- 未观察到 `raft tx command` / END_TRANSACTION 日志；
- Java `TransactionMQProducer` 未发送提交请求。

## 3. 推测

- Raft 模式下 SEND 事务响应字段（msgId/transactionId）与 Java 客户端预期不完全一致；
- 客户端未进入 endTransaction 阶段；
- 需抓包/客户端日志进一步定位。

## 4. 处理

- 回退 broker/main 与 mq-raft；
- 事务消息保持原非 Raft 路径（已验证）。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
