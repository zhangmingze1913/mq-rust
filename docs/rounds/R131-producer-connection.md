# R131 · producerConnection（204）

> 日期：2026-11-25  
> 结果：`mqadmin producerConnection` 列出在线 producer clientId/真实地址/语言/版本。

---

## 1. 实现

- store 新增 producers / producer_addrs；
- 心跳注册 producer 时记录 clientId、peer 地址；
- unregister 清理；
- 204 `GET_PRODUCER_CONNECTION_LIST` 返回：

```json
{"connectionSet":[{"clientId":"...","clientAddr":"127.0.0.1:65244","language":"JAVA","version":409}]}
```

## 2. 验证

```text
mqadmin producerConnection -g mq_rust_producer_hold_group -t MQ_TOPIC

0001  198.18.0.1@16228#12939573753400  127.0.0.1:65244  JAVA  V4_9_8
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       81 项通过
```

## 4. 限制

- 语言/版本固定；
- producer 组来自心跳，非持久化；
- 非正常断开可能残留；
- 未返回 producer 连接数以外的指标。
