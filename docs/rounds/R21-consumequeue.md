# R21 · ConsumeQueue 持久化索引 + 冷读

> 日期：2026-11-25  
> 状态：**Broker 写入 CQ 索引并以 CQ→CommitLog 冷读**；单分片路径已验证。

---

## 1. 改动

- `BrokerStore` 增加 `cq: HashMap<(topic, queue), Mutex<ConsumeQueue>>`；
- 每条消息 append 到 CommitLog 后，写 `cq/<topic>/<queue>.cq`；
- CQ 条目为 20 字节：`commitlog_offset u64 + msg_size u32 + tag_hash u64`；
- `read` 优先用 CQ 定位 CommitLog（`read_at` + `StoredMessage::decode`）；多分片按 `queue_id % shards` 选择日志文件。
- 重启 replay 时：
  - 已有 CQ 长度 > queue_offset → 跳过（不重复写）；
  - 缺 CQ → 重建。

## 2. 测试

```text
test store::tests::message_index_survives_reopen ... ok
test store::tests::consume_queue_persists_offsets ... ok
test store::tests::consume_queue_multi_shard_read ... ok
```

- 写 2 条 → 关闭；
- 重新打开（replay）；
- 再写 1 条；
- CQ 文件 `cq/orders/0.cq` 共 3 条，可读出 3 个 `CqEntry`；
- `store.read` 通过 CQ 冷读返回 3 条消息（`start=0,len=3`）；
- 2 分片场景：queue 1/3 分别落到 shard 1/0，CQ 冷读各返回 1 条；
- **IndexFile**：`msg_id -> (topic, queue, offset)` 写入 `index`，重启后可 O(1) 查询；
- 启动时 `index` 日志重写为最新索引，避免无限增长；
- CQ 冷读必须回填 `queue_offset`/`queue_id`/`physical_offset`（否则 push consumer 因 offset 全为 0 丢消息）；
- 回归：`ORDER RESULT done=true count=50 ordered=true`。

Java SDK：

```text
RESULT received=15 asyncOk=5 send=true
PULL_ONLY received=15 expected=15
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
```

## 4. 限制

| 限制 | 计划 |
|---|---|
| 读路径仍用内存队列 | ✅ R21.1 已接 CQ 冷读 |
| 多 CommitLog 分片 | ✅ 按 `queue_id % shards` 定位 |
| 无 IndexFile（按 key 查询） | ✅ msgId 索引；key 索引待补 |
| 无 CQ 文件轮转/清理 | 后续 |

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`：CQ 打开/追加/去重
- `crates/mq-store/src/lib.rs`：`ConsumeQueue` / `CqEntry`
