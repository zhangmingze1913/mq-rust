# R59 · R58 后全量回归

> 日期：2026-11-25  
> 结果：核心功能全部通过；Tag 首次运行出现一次超时，重跑 3/3 通过（疑似首次 topic 路由建立竞态）。

---

## 1. 回归结果

| 场景 | 结果 |
|---|---|
| Compat | `15/15` |
| Push | `PUSH RESULT received=20 expected=5 ok=true` |
| Batch | 通过 |
| Retry | `attempts=63 ok=true` |
| DLQ | `attempts=34 dlq=32` |
| Tag（首次） | ❌ 超时 |
| Tag（重跑） | ✅ `received=3 ok=true` |
| Tx | `commit=1 rollback=0` |
| Order | `done=true count=50 ordered=true` |
| Delay | `EARLY=0 LATE=1` |
| Inbox | `delivered=3 processed=1` |
| mqadmin clusterList | ✅ |

## 2. Tag 问题

- 首次默认 topic `MQ_RUST_TAG` 订阅/发送时序失败；
- 立即重跑同 topic 通过；
- 新 topic 直跑也通过；
- 判断为首次 topic 路由建立竞态，不是过滤逻辑错误；
- 未在本轮修改，记录为已知 flake。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 4. 限制

- Tag 首次路由竞态未修复；
- 5 万 TPS/严格 HA/容器/TLS 未完成。
