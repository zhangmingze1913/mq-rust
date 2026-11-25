# R83 · Retention 崩溃恢复验证

> 日期：2026-11-25  
> 结果：1000 条、每队列保留 50 → 400 条；kill 重启后仍 400，PullOnly 400/400。

---

## 1. 流程

```text
--cq-segment-entries 100 --retain-messages 50 --retain-interval-ms 1000
发送 1000 条
/stats → messages=400, retained_dropped=600
kill broker
同 store-root 重启
/stats → messages=400, retained_dropped=600
PULL_ONLY received=400 expected=400
```

## 2. 说明

- 重启时 CommitLog 仍含全部 1000 条，replay 后再由 Retention 丢弃到 400；
- CQ 分段/截断与消息数一致；
- 保留策略本身不删除 CommitLog 数据（仅内存/CQ 视图），磁盘仍增长。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```

## 4. 限制

- CommitLog 未按 Retention 删除/轮转；
- 重启会重新 replay 全部历史；
- 长期运行需 CommitLog 分段/清理。
