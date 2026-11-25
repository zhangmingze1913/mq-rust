# R104 · 0.9.0-rc.1 交付冻结

> 日期：2026-11-25  
> 结果：最终报告刷新；Rust 79 项、Java 23 场景；打标签 `v0.9.0-rc.1`。

---

## 1. 本轮工作

- 最终报告补充 R100–R103：
  - SDK 边界 8 类；
  - 定时消息重启 1/1；
  - half 重启后提交 visible=1；
  - 超时回滚 pending=0；
- 明确 SQL92 Pull 首个心跳前版本门控限制；
- 质量门禁复核。

## 2. 交付基线

```text
tag: v0.9.0-rc.1
Rust: 79 tests
Java: 23 scenarios
性能: 1M 8,438 TPS；batch64 24,982；consume 11,130
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 结论

- 功能/SDK 兼容 RC；
- 未达生产 1.0（5 万 TPS、容器、严格 Raft、TLS/gRPC）；
- 后续轮次仅做收口，不再改变协议语义。
