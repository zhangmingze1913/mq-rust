# R247 · base+delta 增量快照

> 日期：2026-11-27  
> 结果：全量模式支持 base snapshot + delta；每次快照只写新增消息 delta，累计 1000 条再 rebase；重启合并 base+delta。

---

## 1. 格式

```text
raft.snapshot         base（完整快照，含 applied/membership）
raft.snapshot.delta   自 base 以来的新增消息
raft.snapshot.meta    applied/membership/base_count/total
```

## 2. 逻辑

- `MQ_SNAPSHOT_FULL=1` 时启用；
- 首次或 delta ≥1000 条：写 base 全量，清 delta；
- 否则只写 delta + meta；
- 启动时 base + delta 合并，meta 恢复 applied/membership；
- install_snapshot 写 base 全量并清 delta；
- 有界尾部模式仍单文件（不使用 delta）。

## 3. 测试

```text
snapshot_full_delta_merges_on_reopen ... ok
1500 base + 50 delta -> 重启 1550 条
```

## 4. 边界

- delta 阈值固定 1000；
- 传输仍为合并后的完整 snapshot（增量在磁盘写，不是增量网络传输）；
- 有界模式不生成 delta。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
```
