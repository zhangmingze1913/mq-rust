# R140 · 最终发布审计

> 日期：2026-11-25  
> 结论：交付物一致；**仍不满足生产 1.0**。

---

## 1. Git

```text
HEAD        b2c7e98 perf: async sampled trace writer; refresh dist
tag         v0.9.0-rc.1 -> b2c7e98d3c032342370ed0d0af6c178758666fd4
status      clean
```

## 2. 分发包

```text
zip         dist/mq-rust-0.9.0-rc.1-win64.zip
SHA256      78C12EE06E9A941C13C47AE3D754F92E4F7216E799C3FAC2E774ABFF101D6C9C
docs check  OK（SHA/测试数/轮次/链接一致）
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 4. 能力边界

```text
已验证：协议/SDK/事务/顺序/延时/批量/死信/重试/Inbox/管理面/轨迹/HA 基础
未达成：5 万 TPS、容器实测、严格 Raft、TLS/gRPC、group ACL
```

## 5. 判定

```text
0.9.0-rc.1 可作为 RC 交付与验收
不得声明生产 1.0
```
