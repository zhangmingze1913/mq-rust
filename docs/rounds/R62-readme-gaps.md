# R62 · README 缺口与优先级刷新

> 日期：2026-11-25  
> 结果：README 的“已知缺口/优先级”与当前实现对齐（同步 ack、AccessKey、Retention、Tag flake）。

---

## 1. 更新内容

| 项 | 旧描述 | 新描述 |
|---|---|---|
| 性能 | 仅单条 9,347 | 单条 9,347 + 批量 24,982 |
| HA | 异步主从、无防脑裂 | 异步/同步 ack + 事件/epoch 栅栏；无共识/多数派 |
| 安全 | 无 ACL/TLS | AccessKey 原型（无 Signature）；无 TLS |
| 存储 | 无轮转压缩 | Retention 有；CQ 全量重写、无分段 |
| 测试 | 未提 flake | Tag 首次超时，复测 5/5，根因未定位 |

## 2. 后续优先级

```text
P0 性能 / 精确 ack+多数派
P1 容器 / ACL 签名+TLS / 分段 CQ
P2 Inbox 自动 / 轨迹
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```
