# R100 · SDK 边界场景补充验证

> 日期：2026-11-25  
> 结果：Push/Retry/Rebalance/TxTimeout/TxCheck/Idem/DLQ/SQL92-Push 通过；Pull SQL92 暴露客户端版本门控时序问题。

---

## 1. 结果

| 场景 | 结果 |
|---|---|
| Push | `5/5 ok=true` |
| Retry | `attempts=2 ok=true` |
| Rebalance | `c1=10 c2=5 ok=true` |
| TxTimeout | `visible=0` |
| TxCheckBack | `checks=1 visible=1` |
| Idempotent | `count=1` |
| DLQ | `attempts=2 dlq=1`，topic `%DLQ%...` |
| SQL92 Push | `count=1` |
| SQL92 Pull | ❌ 客户端报 “broker does not upgrade…” |

## 2. SQL92 Pull 分析

```text
异常：
The broker[broker-a, 0] does not upgrade to support for filter message by SQL92
位置：
PullAPIWrapper.pullKernelImpl
```

- Broker 心跳响应已设置 `version=409`（RocketMQ 4.9.8）；
- PullAPIWrapper 用 `brokerVersionTable` 判断，该表由心跳响应填充；
- Pull 客户端启动后**立即 pull**，心跳尚未返回，版本为 0，触发拒绝；
- SQL92 Push 测试先 sleep 3s，心跳完成后通过。

## 3. 结论

- 服务端 SQL92 过滤本身可用（Push 通过）；
- Pull 客户端需等待首次心跳后再发 SQL92 pull；
- 这是客户端时序/测试问题，不是 broker 过滤错误；但需在文档中标注。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 未改 Java 测试的等待逻辑；
- StartDeliver 未在本轮复跑；
- SQL92 规则覆盖仍有限。
