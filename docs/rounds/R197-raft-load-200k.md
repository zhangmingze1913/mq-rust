# R197 · Raft 20 万条长跑

> 日期：2026-11-26  
> 结果：单节点 Raft batch256 连续写入 206,336 条，发送失败 0，约 3.0–3.9k msg/s。

---

## 1. 配置

```text
单节点 Raft
batchSize 256 / messageSize 128 / threadCount 8
retain-messages 200,000
trace off
```

## 2. 结果

```text
messages: 206,336
body_bytes: 26,411,008
commitlog_bytes: 60,662,784
Send Failed: 0

MPS 3847 / 3856 / 3736 / 3023
```

## 3. 对比

```text
非 Raft batch256 峰值 37,095
Raft 三副本 batch256 ~1,929
Raft 单节点 batch256 ~3.0–3.9k
```

Raft 提交/apply/日志持久化是主要成本。

## 4. 说明

- 单节点，无网络复制；
- 未做 1M Raft（约 5–9 分钟）；
- 未做消费端长跑；
- 5 万仍不可达。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
