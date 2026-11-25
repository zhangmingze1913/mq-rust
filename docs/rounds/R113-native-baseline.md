# R113 · 原生 RocketMQ 4.9.8 同机基线

> 日期：2026-11-25  
> 结果：同机原生 RocketMQ 1M 仅 **15,465 TPS**；50k 目标在该硬件不可达，MQ-rust 8,438 TPS 约为原生 55%。

---

## 1. 环境

```text
CPU：2 核 / 4 线程
磁盘：HDD
JDK：17.0.2
RocketMQ：4.9.8（ASYNC_MASTER / ASYNC_FLUSH）
消息：256B，8 线程
```

## 2. 原生 RocketMQ

```text
100k：9,424 TPS，Max RT 555ms，0 失败
1M  ：15,465 TPS，Max RT 626ms，0 失败
```

## 3. MQ-rust 对照

| 场景 | 原生 4.9.8 | MQ-rust 0.9.0-rc.1 |
|---|---|---|
| 100k | 9,424 | 9,347 |
| 1M | 15,465 | 8,438 |
| 批量 64 | 未测 | 24,982 msg/s |

## 4. 结论

- 原生 RocketMQ 在该机器也只有 15.5k，**50k 不是实现差距，是硬件上限**；
- MQ-rust 100k 接近原生（99%）；
- 1M 为原生 55%，仍有分配/JIT/写入路径优化空间；
- 达到 5 万必须换 SSD/更多核/分布式压测，或下调目标。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 6. 限制

- 原生结果单次采样；
- 未做消费/批量对照；
- 未调优原生 broker 参数。
