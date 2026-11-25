# R88 · CommitLog 段回收 + 队列基址持久化

> 日期：2026-11-25  
> 结果：Retention 丢弃 196k 后，CommitLog 从 ~84MB 降到 **16.29MB**；重启仍 4,000/4,000，offset 连续。

---

## 1. 机制

- CommitLog 每段写 `<path>.meta`（首段全局 base）；
- `trim_prefix(cutoff)` 删除整段、重命名后续段、持久化 base；
- 全局 offset 不变，最后一段保留；
- `retention.json` 持久化每 topic/queue 的 `base_offset`；
- `open` replay 后加载 retention：
  - 设置 queue base；
  - 消息 `queue_offset` 增加 delta；
  - 更新 msgId 索引。

## 2. 验证

```text
200k / 256B / 8 线程：8,780 TPS，0 失败
retention 500/队列：保留 4,000，丢弃 196,000
commitlog_bytes：~84MB → 16,290,000
PULL_ONLY 4000/4000
重启后：messages=4000，commitlog_bytes=16.29MB
PULL_ONLY 4000/4000
RANGE queue min≈33,270 max≈33,770（绝对偏移）
```

单测：

```text
commitlog_trim_prefix_keeps_offsets ... ok
retention_trims_commitlog_and_persists_base ... ok
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       77 项通过
```

## 4. 限制

- 只删整段，最后一段（含旧数据）保留，磁盘非精确回收；
- retention.json/commitlog meta 非原子写；
- 索引/位点日志未回收；
- 半消息/重试/延时日志未随主日志回收。
