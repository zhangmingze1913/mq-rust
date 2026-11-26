# R193 · Raft 快照触发与日志压缩

> 日期：2026-11-26  
> 结果：openraft 每 2000 条日志触发快照；DiskLogStore 在 truncate/purge 时重写 JSONL 完成压缩。

---

## 1. 配置

```text
snapshot_policy: LogsSinceLast(2000)
max_in_snapshot_log_to_keep: 100
purge_batch_size: 16
```

`MessageSM` 快照写入 `raft.snapshot`（R192）。

## 2. 日志压缩

`DiskLogStore::rewrite()`：

- 按内存 state 重写 `raft.log`；
- 保留 vote、未 purge entries、purged id；
- truncate/purge 后调用；
- tmp + rename 原子替换。

## 3. 效果

- 已快照的旧日志可被 purge；
- 日志文件不再无限增长；
- 重启时 snapshot + 剩余日志恢复。

## 4. 边界

- 压缩仍是全量重写，非分段文件；
- 未做 1M 长跑验证压缩频率；
- leader 崩溃事务恢复未做。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
