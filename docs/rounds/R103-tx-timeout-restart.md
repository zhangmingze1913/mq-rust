# R103 · 事务超时回滚重启 + tx_pending 修复

> 日期：2026-11-25  
> 结果：half 超时后回滚，messages=0、tx_timeout_total=1、tx_pending=0；修复重启后 pending 为负。

---

## 1. 测试

```text
send half（tx-timeout=30s）
kill broker
重启：--tx-timeout-ms 2000
等待 4s
/stats → messages=0
/metrics → tx_timeout_total=1, tx_pending=0
PULL_ONLY received=0 expected=0
```

## 2. 修复

- 新增 `BrokerStore::pending_half_count()`；
- 启动时用 replay 后的 half 数量初始化 `tx_pending`；
- 修复前重启后超时回滚会把 gauge 减到 **-1**。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 仅单 broker；
- 未验证 check-back 耗尽后的回滚重启；
- 超时精度 1s。
