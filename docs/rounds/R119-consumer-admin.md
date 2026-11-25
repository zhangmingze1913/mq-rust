# R119 · mqadmin consumerProgress / consumerConnection

> 日期：2026-11-25  
> 结果：`consumerProgress` 输出 5 个队列进度；`consumerConnection` 列出在线 clientId/language/version。

---

## 1. 验证

```text
固定 group：mq_rust_offset_progress_group
Push 消费 5 条后：

consumerProgress：
  MQ_OFFSET_PROGRESS broker-a 1 1 1 0 ...
  ...
  Consume TPS: 0.00
  Diff Total: 0

consumerConnection：
  198.18.0.1@14128#...  127.0.0.1:0  JAVA  V4_9_8
```

## 2. 实现

- 203 从 `consumers` 读取 group 的 clientId 列表；
- 返回 `connectionSet` + `consumerTable`；
- `clientAddr` 暂填 `127.0.0.1:0`（未保存 peer）；
- 202 `topicStatus` 已在 R118 实现；
- 208 `GET_CONSUME_STATS` 原有实现返回 offsetTable。

## 3. 新增测试

`tests/compat/src/MqRustOffsetProgress.java`：固定 group 发送 5 条并消费 5 条，供 mqadmin 检查。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- consumerConnection 的 ConsumeType/MessageModel/ConsumeFromWhere 显示 null；
- subscriptionTable 为空；
- clientAddr 非真实地址；
- 无消费者时仍返回 206。
