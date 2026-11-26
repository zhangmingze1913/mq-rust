# R192 · Raft 状态机快照持久化

> 日期：2026-11-26  
> 结果：MessageSM 快照写入 `raft.snapshot`，重启后可恢复消息状态；新增快照重启单测。

---

## 1. 实现

- `MessageSM` 新增 `snapshot_path`；
- `build_snapshot` / `install_snapshot` 原子写 JSON：

```text
{ messages, applied, membership }
```

- 启动时加载快照，恢复 messages/applied/membership；
- broker 使用：

```text
store_root/raft.snapshot
```

- Raft 日志仍为 append-only JSONL。

## 2. 单测

```text
cargo test -p mq-raft
snapshot_survives_reopen ... ok
three_nodes_replicate_message_log ... ok
2 passed
```

## 3. 边界

- openraft 触发快照的策略仍为默认值；
- 未做分段日志/压缩；
- 快照与日志清理的联合恢复未做 1M 长跑；
- 尚未在 broker 进程重启中触发快照场景。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
