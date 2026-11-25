# R60 · Tag 过滤稳定性复测

> 日期：2026-11-25  
> 结果：新建 Broker，Tag 过滤 5 次（默认 topic + 3 个新 topic + 默认重跑）全部 3/3 通过；此前 R59 的一次超时未复现。

---

## 1. 结果

```text
MQ_TAG110        received=3 ok=true
MQ_RUST_TAG      received=3 ok=true
MQ_TAG110A       received=3 ok=true
MQ_TAG110B       received=3 ok=true
MQ_TAG110C       received=3 ok=true
```

## 2. 分析

- 过滤逻辑本身正常（只收到 TagA，TagB 未投递）；
- R59 失败为一次性超时，可能与首次 topic 路由/环境负载有关；
- 未定位到确定性代码缺陷；
- 保留为已知 flake，未改代码。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 4. 建议

- CI 中对首次 topic 测试增加一次重试；
- 或发送端先预热 topic 再启动消费者。
