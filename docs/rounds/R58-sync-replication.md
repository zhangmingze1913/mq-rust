# R58 · 同步复制 ACK

> 日期：2026-11-25  
> 结果：`--sync-replicate` 时发送等待副本 ack；有从节点 15/15 成功，从节点被杀后发送超时失败。

---

## 1. 验证

主从连接：

```text
RESULT received=15 asyncOk=5 send=true
GET /replica → {"acked":15}
```

杀掉 Slave 后：

```text
发送异常（replica ack timeout，客户端重试/失败）
```

## 2. 机制

- `--sync-replicate` + `--replica-timeout-ms`（默认 1000）；
- 每次 append 记录发送前 `replica_acked`；
- broadcast 后等待 ack 增长，超时返回 `Store("replica ack timeout")`；
- replay 消息不等待（`persist=false`）。

## 3. 限制

| 限制 | 说明 |
|---|---|
| 非严格 quorum | 只要求 ack 增长，未按消息/队列精确确认 |
| 已持久化后失败 | 超时返回错误，但消息已写 CommitLog；客户端重试靠 msgId 去重 |
| 单副本 | 多副本/多数派未实现 |
| 主从非共识 | 仍依赖 Namesrv 路由/栅栏 |
| 吞吐下降 | 每次发送等待 RTT |

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`：`set_sync_replicate` / ack 等待
- `crates/mq-broker/src/main.rs`：`--sync-replicate`
