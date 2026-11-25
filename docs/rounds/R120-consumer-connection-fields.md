# R120 · ConsumerConnection 完整字段

> 日期：2026-11-25  
> 结果：`mqadmin consumerConnection` 正确显示订阅、ConsumeType、MessageModel、ConsumeFromWhere。

---

## 1. 修复

原 203 返回错误结构：

```json
{"connectionSet":[...],"consumerTable":{...},"subscriptionTable":{}}
```

`ConsumerConnection` 实际字段是：

```text
connectionSet
subscriptionTable
consumeType
messageModel
consumeFromWhere
```

没有 `consumerTable`，导致 mqadmin 显示 null。现改为：

```json
{
  "connectionSet":[...],
  "subscriptionTable":{
    "MQ_OFFSET_PROGRESS":{ "topic":"MQ_OFFSET_PROGRESS","subString":"*",... },
    "%RETRY%group":{...}
  },
  "consumeType":1,
  "messageModel":1,
  "consumeFromWhere":0
}
```

## 2. 验证

```text
#ClientId ... #Language JAVA #Version V4_9_8

Below is subscription:
MQ_OFFSET_PROGRESS                      *
%RETRY%mq_rust_offset_progress_group    *

ConsumeType: CONSUME_PASSIVELY
MessageModel: CLUSTERING
ConsumeFromWhere: CONSUME_FROM_LAST_OFFSET
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- clientAddr 仍为占位 127.0.0.1:0；
- subscriptionTable 来自 broker 已注册订阅；
- 无在线消费者时仍返回空表。
