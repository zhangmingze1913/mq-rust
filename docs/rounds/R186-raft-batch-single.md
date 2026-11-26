# R186 · Raft 批量单条日志恢复 + 性能

> 日期：2026-11-26  
> 结果：`RaftCommand::Batch` 在 append-only DiskLogStore 下工作；batch256 约 1.9k msg/s，无失败。

---

## 1. 修复

R185 的挂起与当时 DiskLogStore **每次 append 全量重写 JSON** 有关；R181 后改为 append-only JSONL，重新接入单条批量命令：

```text
SEND_BATCH -> RaftCommand::Batch { messages }
          -> 一次 client_write
          -> MessageSM.apply 展开逐条 apply
```

## 2. 功能

```text
MqRustBatchSmoke
BATCH RESULT received=5
/stats messages=5
```

## 3. 性能

```text
batch256 -s128 -w8
MPS 1173 / 1791 / 1727 / 1929
Send Failed 0
Average Message RT ~4.5–4.9 ms
```

对比：

- 非 Raft batch256 峰值 37,095；
- Raft 单条批量约 **1.9k**；
- 主要成本：Raft commit + 每写 apply + 本地 CommitLog。

## 4. 结论

- 批量单条 Raft 日志可用；
- Raft 模式吞吐远低于非 Raft，不能用于 5 万目标；
- 5 万仍需非 Raft 路径或硬件/异步提交。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
