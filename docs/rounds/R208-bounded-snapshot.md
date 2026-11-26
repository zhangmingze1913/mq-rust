# R208 · 有界快照（尾部 1000 条）

> 日期：2026-11-26  
> 结果：Raft 快照不再全量序列化所有消息，只保留尾部 1000 条 + applied/membership；新增单测。

---

## 1. 变更

- `SnapshotFile` 增加 `total_messages`；
- `build_snapshot` 只序列化最后 1000 条消息；
- `install_snapshot` 只恢复尾部；
- snapshot 文件大小有上界，避免 50 万+ 消息全量写入造成长暂停。

## 2. 语义

- broker 的真实消息数据仍由本地 CommitLog/CQ 恢复；
- Raft 状态机快照只保留诊断尾部 + applied 水位；
- 新节点无法通过快照获得完整历史消息（与现有静态集群一致）。

## 3. 验证

```text
snapshot_tail_is_bounded ... ok
snapshot_survives_reopen ... ok
mq-raft 3 passed
全量 86 项
```

## 4. 边界

- 不是完整状态机快照，属于有界诊断快照；
- 动态加节点/历史迁移不支持；
- 快照与 CommitLog 的强一致仍需后续设计。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（86 项）
```
