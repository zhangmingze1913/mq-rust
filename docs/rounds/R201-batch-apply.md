# R201 · 批量 apply 优化（3 副本 0 失败）

> 日期：2026-11-26  
> 结果：Raft BATCH 改为一次批量 append；三副本 60s 写入 118,272 条，0 失败，三副本一致。

---

## 1. 变更

- `BrokerStore::append_batch`：一次 CommitWriter Batch job 持久化整批；
- `WriteCmd::Batch` + `CommitWriter::submit_batch`；
- `MessageSM` 增加 `BatchApplier`；
- Raft `RaftCommand::Batch` 只调用一次 batch applier；
- broker applier 调 `store.append_batch`。

## 2. 结果

```text
3 副本 Raft，batch256，60s
A/B/C messages = 118,272（完全一致）
Send Failed = 0
MPS 2009 / 2148 / 2212
Average RT ~0.94–1.0s
```

对比 R198（120s，26 次失败者）：

```text
R198: 137,984 / Send Failed 26 / 有超时
R201: 118,272 / Send Failed 0 / 60s
```

吞吐约 1.97k msg/s，且无失败。

## 3. 说明

- 批量持久化减少 256 次 writer 往返；
- 单条/延时/事务仍走单条 applier；
- 三副本一致；
- 未做 1M 三副本。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
BATCH/TX smoke 通过
```
