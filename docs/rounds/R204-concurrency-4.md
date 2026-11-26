# R204 · 并发 4 消除超时

> 日期：2026-11-26  
> 结果：3 副本 batch256、4 并发线程，120s 写入 226,816 条，0 失败，三副本一致。

---

## 1. 结果

```text
3 副本 Raft，batch256，4 threads，120s
A/B/C messages = 226,816（完全一致）
Send Failed = 0
MPS 1849–2104
Average RT ~0.49–0.54s
Max RT 1894ms（低于 3s 超时）
```

## 2. 对比

```text
8 threads 240s: 444,928 / 12 失败 / max RT 2893ms
4 threads 120s: 226,816 / 0 失败 / max RT 1894ms
```

并发从 8 降到 4，单批排队减少，RT 远离 3s 超时。

## 3. 结论

- 生产建议：Raft batch256 使用 4 个并发发送线程；
- 吞吐仍约 1.9–2.1k msg/s；
- 8 并发适合短时压测，但会偶发超时；
- 未改代码，仅配置调优。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
