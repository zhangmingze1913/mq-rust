# R155 · SSD 路径性能（负结果）

> 日期：2026-11-25  
> 结果：C: SATA SSD（剩余 7.8GB）batch256 仅 12–18k msg/s，低于 G: HDD 的 37k。

---

## 1. 环境

```text
CPU  i5-4210M 2C/4T
RAM  11.7GB
Disk C: SSD 128G SATA，剩余 7.8GB
Disk G: HGST HDD 500G
```

## 2. 结果

```text
store-root C:/mq-rust-data/ssd，batch256 -s128 -w8
Send TPS 71  MPS 18186
Send TPS 58  MPS 15038
Send TPS 47  MPS 12203
Send TPS 50  MPS 12891
```

对比：

| 存储 | batch256 |
|---|---|
| G: HDD | 37,095 峰值 |
| C: SSD | 12,891–18,186 |

## 3. 分析

- C: 仅剩 7.8GB，SATA SSD，可能受 Windows Defender/空间压力影响；
- CPU 仍为 4 线程，单写线程/序列化瓶颈未变；
- 不能把“换 SSD”简单等同于吞吐提升，需 NVMe + 充足空间 + 杀软排除 + 多核。

## 4. 结论

- 5 万仍未达成；
- 需要实际提供 ≥8 核 + NVMe（≥100GB 空闲）或 2–4 broker 集群；
- 仅现有 C: SSD 不满足。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
