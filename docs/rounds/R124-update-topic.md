# R124 · mqadmin updateTopic

> 日期：2026-11-25  
> 结果：`mqadmin updateTopic` 成功创建空 topic；首次发送后 `topicList` 可见。

---

## 1. 实现

- 请求码 17 `UPDATE_AND_CREATE_TOPIC`；
- 从 ext 读取 `topic`；
- `BrokerStore::create_topic` 创建 8 队列空 TopicState；
- 重复创建幂等。

## 2. 验证

```text
mqadmin updateTopic -c DefaultCluster -t MQ_CREATED
  -> create topic to 127.0.0.1:10921 success
  TopicConfig [readQueueNums=8, writeQueueNums=8, perm=RW-...]

mqadmin sendMessage -t MQ_CREATED
  -> SEND_OK

mqadmin topicList
  MQ_CREATED
  TBW102
```

空 topic 在首次发送前不会出现在 namesrv 的 topicList 中。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 空 topic 未同步 namesrv 路由；
- 未持久化空 topic，重启丢失；
- 未实现 deleteTopic；
- 队列数固定 8。
