# R24 · 持久化消费 Inbox

> 日期：2026-11-25  
> 状态：**应用可通过 `POST /inbox/{group}/{msg_id}` 做幂等判重，Inbox 重启不丢**；Broker 不自动调用。

---

## 1. API

```text
POST http://127.0.0.1:9091/inbox/{group}/{msg_id}
```

首次：

```json
{"duplicate":false,"fresh":true}
```

重复：

```json
{"duplicate":true,"fresh":false}
```

## 2. 持久化

- 文件：`--inbox-path`（默认 `./data/inbox.log`）；
- 每行 `group\tmsg_id`；
- 启动时加载到 `HashSet`；
- 写入后 `flush`，重启后仍判重。

## 3. 验证

```text
第一次：{"duplicate":false,"fresh":true}
第二次：{"duplicate":true,"fresh":false}
重启后：{"duplicate":true,"fresh":false}
```

单测：

```text
test inbox::tests::inbox_dedups_and_survives_reopen ... ok
```

SDK 端到端：

```text
INBOX RESULT delivered=3 processed=1 done=true
```

`MqRustInboxSmoke`：3 条相同业务 key 的消息，消费者先 `POST /inbox` 判重，仅处理 1 条。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       59 项通过
```

## 5. 限制

| 限制 | 计划 |
|---|---|
| 需应用显式调用 | ✅ SDK demo 已封装调用 |
| 单机文件 | 分布式去重表 |
| 无 TTL/清理 | 定期压缩 |
| Inbox 与消费位点独立 | 对账 |

## 6. 涉及文件

- `crates/mq-broker/src/inbox.rs`
- `crates/mq-broker/src/admin.rs`
- `crates/mq-broker/src/main.rs`
