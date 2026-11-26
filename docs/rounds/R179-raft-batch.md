# R179 · Raft BATCH 接入

> 日期：2026-11-26  
> 结果：SEND_BATCH 在 Raft 模式下逐条 `client_write`，批量 5 条全部 apply；SDK 收到 5 条。

---

## 1. 变更

`SEND_BATCH_MESSAGE`：

```text
if raft:
  leader check
  for each message: raft.write(MessageCommand)
  message_by_id 取回 offset
else:
  store.append
```

## 2. 验证

```text
MqRustBatchSmoke
BATCH RESULT received=5

/stats messages=5
```

## 3. 代价

- 批量不是单条 Raft 日志，而是 5 条日志；
- 吞吐低于原批量路径；
- 后续可合并为 `Vec<MessageCommand>` 单条日志。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```

## 5. 未完成

- 批量日志合并；
- 延时/事务 Raft；
- 持久化与重启追平。
