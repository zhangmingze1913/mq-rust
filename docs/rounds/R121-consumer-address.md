# R121 · ConsumerConnection 真实地址

> 日期：2026-11-25  
> 结果：203 返回真实 `clientAddr`（如 127.0.0.1:56847），不再固定为 127.0.0.1:0。

---

## 1. 改动

- 新增 `consumer_addrs` 映射；
- 心跳注册时传入 `peer`（SocketAddr）；
- `register_consumer_addr` 记录 clientId → 地址；
- `unregister_client` 清理地址；
- 203 使用 `consumer_addr(client_id)`。

## 2. 验证

```text
#ClientId                            #ClientAddr          #Language  #Version
198.18.0.1@13948#6067624922000       127.0.0.1:56847      JAVA       V4_9_8
198.18.0.1@1548#6089983991600        127.0.0.1:56857      JAVA       V4_9_8
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 非正常断开可能留下旧 clientId（60s 过滤仅用于 consumer_ids，203 未过滤）；
- 无真实客户端语言/版本，固定 JAVA/V4_9_8；
- 订阅表来自 broker 注册。
