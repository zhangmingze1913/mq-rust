# R78 · 消息轨迹 `/trace/{msgId}`

> 日期：2026-11-25  
> 结果：Broker 记录发送/拉取事件，`GET /trace/{msgId}` 可查同一消息的 send/pull 轨迹。

---

## 1. 验证

发送（AclClientRPCHook）：

```json
[{"event":"send","msg_id":"...","topic":"MQ_TRACE","queue_id":5,"queue_offset":0,"timestamp":...}]
```

消费后：

```json
[
 {"event":"send", ...},
 {"event":"pull","group":"mq_rust_pull_only","queue_id":5,"queue_offset":0,...}
]
```

## 2. 实现

| 项 | 说明 |
|---|---|
| `TraceLog` | 有界 `VecDeque`（默认 10000） |
| 事件 | send / pull |
| 字段 | msg_id/topic/queue/offset/group/timestamp |
| Admin | `GET /trace/{msg_id}`（受 `--admin-token` 保护） |
| 容量 | `--trace-capacity` |

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```

## 4. 限制

- 内存轨迹，重启丢失；
- 仅 send/pull，无事务/重试/死信事件；
- 全表扫描 `msg_id`，容量大时 O(n)；
- 无持久化/查询 API 分页。
