# R41 · 崩溃恢复验证（CQ 修复后）

> 日期：2026-11-25  
> 结果：写入 15 条后 kill Broker，重启恢复 15/15。

---

## 1. 流程

```text
Compat 写入 15 条 → RESULT received=15
kill -9 broker (pid 2948)
同 store-root 重启
PULL_ONLY received=15 expected=15
```

## 2. 涉及机制

- CommitLog CRC/扫描恢复；
- CQ 启动修复（R38）；
- msgId 索引重建；
- 审计/outbox 持久化。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       62 项通过
```

## 4. 结论

CQ 冷读/修复后，崩溃重启仍能完整恢复消息；  
未覆盖：刷盘策略极端场景、磁盘损坏、长跑崩溃循环。
