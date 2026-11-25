# R125 · mqadmin deleteTopic

> 日期：2026-11-25  
> 结果：`deleteTopic` 从 broker 与 namesrv 删除，topicList 不再显示。

---

## 1. 实现

- Broker 215 `DELETE_TOPIC_IN_BROKER`：
  - 删除 TopicState；
  - 清理 msg_index/dedup/offsets/CQ 缓存；
  - 删除 `cq/<topic>/` 目录。
- Namesrv 216 `DELETE_TOPIC_IN_NAMESRV`：
  - 从 `state.topics` 移除。

## 2. 验证

```text
mqadmin updateTopic -t MQ_DEL3 -> success
mqadmin sendMessage -t MQ_DEL3 -> SEND_OK

topicList BEFORE:
  MQ_DEL3
  TBW102

mqadmin deleteTopic -t MQ_DEL3
  delete topic [MQ_DEL3] from cluster [DefaultCluster] success.
  delete topic [MQ_DEL3] from NameServer success.

topicList AFTER:
  TBW102
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- CommitLog 物理数据未删除；
- 删除后同名 topic 可重新创建；
- 多 broker/多副本未同步删除；
- 正在消费的客户端需重新路由。
