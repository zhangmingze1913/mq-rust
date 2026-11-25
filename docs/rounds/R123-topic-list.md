# R123 · mqadmin topicList 兼容

> 日期：2026-11-25  
> 结果：`mqadmin topicList` 返回真实 topic，不再只有 TBW102。

---

## 1. 实现

请求码 21 `GET_ALL_TOPIC_CONFIG`：

```json
{
  "topicList": ["MQ_TOPICLIST", "TBW102"],
  "brokerAddr": "127.0.0.1:10921"
}
```

- topic 来自 store 内存表；
- 始终补 `TBW102`。

## 2. 验证

```text
mqadmin sendMessage -t MQ_TOPICLIST -> SEND_OK
mqadmin topicList：
  MQ_TOPICLIST
  TBW102
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 只列 broker 已创建 topic；
- 未实现 updateTopic/deleteTopic；
- 未返回 topic 配置（队列数/权限）。
