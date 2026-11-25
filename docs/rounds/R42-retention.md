# R42 · 每队列消息保留上限（Retention）

> 日期：2026-11-25  
> 结果：新增 `--retain-messages`，每队列只保留最新 N 条，base offset 前移，CQ 同步重写。

---

## 1. 机制

- `QueueState.base_offset`：队列绝对起始偏移；
- `retain(max)`：`messages.drain(0..len-max)`，`base_offset += dropped`；
- `repair_cq` 重写 CQ 为保留消息；
- `read` 将小于 base 的 offset 夹到 base；
- `min_offset` 返回 base；
- 后台每 `--retain-interval-ms` 执行一次；
- `--retain-bytes` 按每队列 body 总字节额外限制；
- `--retain-max-age-ms` 按消息 `born_timestamp` 的 TTL 限制。

## 2. 验证

发送 15 条（8 队列），`--retain-messages 1`：

```text
/stats: {"messages":8,"queues":8,...}
PULL_ONLY received=8 expected=8
```

即每队列保留 1 条，旧消息被丢弃，CQ/索引/拉取一致。

单测：

```text
test store::tests::retention_advances_base_offset ... ok
test store::tests::retention_by_bytes ... ok
test store::tests::retention_by_age ... ok
```

- 10 条保留 5；
- `min_offset=5`，读回 offset 5..9；
- CQ 5 条；
- `RET-0` 查不到、`RET-9` 可查；
- 字节限制：10 条 ×5B，`retain(0,15,0)` 丢弃 7 条，保留 3 条；
- TTL：前 7 条 `born_timestamp=1`，后 3 条为当前时间，`retain(0,0,1000)` 丢弃 7 条。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 4. 限制

| 限制 | 计划 |
|---|---|
| 已支持条数/字节/TTL | ✅ |
| 丢弃后旧 consumer offset 会被夹到 base | 客户端需处理 offset 前移 |
| CQ 全量重写，大队列有停顿 | 分段/增量清理 |
| 索引/位点不清理 | 后续对齐 |

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`：`base_offset` / `retain`
- `crates/mq-broker/src/main.rs`：`--retain-messages` / `--retain-interval-ms`
