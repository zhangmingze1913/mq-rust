# R111 · 交付索引与收口

> 日期：2026-11-25  
> 结果：新增 [docs/20-交付索引.md](../20-交付索引.md)，汇总源码、分发包、文档、门禁、核心验收与 1.0 差距。

---

## 1. 交付

```text
tag: v0.9.0-rc.1
commit: 1cafc59
zip SHA256: B56D17C467DD49C75A6E6A3069E867832C9A0CBED418A702B7A4F64FF1E077DA
Rust: 79 tests
Java: 23 scenarios
```

## 2. 结论

- 功能/SDK 兼容 RC；
- 1M 8,438 TPS、0 失败；
- 容器/5 万 TPS/严格 Raft/TLS 未完成；
- 不声明生产 1.0。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 说明

目标未完成，保持 active；不再修改协议语义。
