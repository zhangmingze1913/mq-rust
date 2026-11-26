# R185 · Raft 批量单条日志（负结果，已回退）

> 日期：2026-11-26  
> 结果：尝试把 SEND_BATCH 合并为单条 Raft 日志；单节点 broker 批量发送挂起，代码已回退。

---

## 1. 尝试

- 新增 `RaftCommand::Batch { messages }`；
- `MessageRaft::write_batch` 一次 `client_write`；
- `MessageSM.apply` 展开批量逐条 apply；
- SEND_BATCH 一次提交，再逐条 `message_by_id`。

## 2. 结果

```text
MqRustBatchSmoke -> 挂起
producer registered
/stats messages=0
```

未观察到 apply/write 日志，client_write 未完成。

## 3. 处理

- 回退 `crates/mq-broker/src/server.rs` 与 `crates/mq-raft/src/lib.rs`；
- 保留 R179 的逐条 Raft 批量路径（已通过）。

## 4. 结论

- 单条批量命令在 openraft 0.10 + 自定义状态机下存在未定位阻塞；
- 逐条批量虽多日志，但已验证；
- 后续可在独立测试中定位 `RaftCommand` 反序列化/apply 回调问题。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
