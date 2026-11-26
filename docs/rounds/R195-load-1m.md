# R195 · 1M+ 批量长跑

> 日期：2026-11-26  
> 结果：非 Raft batch256 连续写入 1,253,376 条，发送失败 0。

---

## 1. 配置

```text
broker: 127.0.0.1:10921
batchSize: 256
messageSize: 128
threadCount: 8
retain-messages: 3,000,000
flush-interval-ms: 1000
trace: off
```

## 2. 结果

```text
messages: 1,253,376
topics: 1 (MQ_LOAD1)
body_bytes: 160,432,128
commitlog_bytes: 363,479,330
Send Failed: 0
```

分段输出：

```text
MPS 24,177 / 19,436 / 3,731 / 29,027 / 28,431
```

- 60s 首轮约 993k；
- 追加约 12s 后达到 1.25M；
- 平均约 17–29k msg/s，未达到 37k 峰值（长跑 + retention/磁盘抖动）。

## 3. 说明

- 非 Raft 路径；
- 写入全部成功，无失败；
- 未做 1M 端到端消费验证；
- 未做 Raft 1M（Raft ~1.9k，需约 9 分钟）。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
