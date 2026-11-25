# R79 · 事务/死信轨迹扩展

> 日期：2026-11-25  
> 结果：事务消息轨迹包含 `tx_prepare`、`tx_commit`/`tx_rollback`、`pull`；死信路由记录 `dlq`。

---

## 1. 验证

事务提交后查询 `/trace/{txId}`：

```json
[
 {"event":"tx_prepare","topic":"MQ_TXTRACE_C","queue_id":5,"queue_offset":0,...},
 {"event":"tx_commit","topic":"MQ_TXTRACE_C","queue_id":5,"queue_offset":0,...},
 {"event":"pull","group":"mq_rust_tx_pull_...","queue_id":5,...}
]
```

普通 send/pull 轨迹保持 R78 行为。

## 2. 实现

| 事件 | 位置 |
|---|---|
| `tx_prepare` | SEND 且 `TRAN_MSG=true` |
| `tx_commit` | END_TRANSACTION action=8 成功 |
| `tx_rollback` | END_TRANSACTION action=12 成功 |
| `dlq` | 重试超限路由到 `%DLQ%` |
| `send`/`pull` | R78 |

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```

## 4. 限制

- 内存轨迹，重启丢失；
- 无按 topic/时间查询/分页；
- rollback 事件的 topic 为空（half 已删除）；
- 无 trace 持久化。
