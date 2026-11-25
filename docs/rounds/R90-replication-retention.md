# R90 · 同步复制 + Retention 双副本一致性

> 日期：2026-11-25  
> 结果：master/slave 均保留 4,000 条、丢弃 1,000；slave 重启仍 4,000。同步复制吞吐 450 TPS。

---

## 1. 参数

```text
master: --sync-replicate --replica-timeout-ms 3000 --replica-listen 10931
slave:  --replica-of 127.0.0.1:10931
both:   queues=8, retain=500/队列, CQ=1000, CL segment=64MiB
发送 5,000 × 256B / 8 线程
```

## 2. 结果

```text
生产：5,000，TPS 450，0 失败，平均 RT 17.6ms
MASTER /stats：
  messages=4000, retained_dropped=1000, commitlog_bytes=2,085,000
SLAVE /stats：
  messages=4000, retained_dropped=1000, commitlog_bytes=2,085,000
slave 重启：
  messages=4000, retained_dropped=1000, commitlog_bytes=2,085,000
```

## 3. 说明

- 双副本 Retention 独立执行但结果一致；
- 同步复制每条等待 ack，吞吐降至 450 TPS；
- slave 重启从本地日志恢复 4,000 条；
- 段大小 64MiB > 数据量，未触发 trim。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       77 项通过
```

## 5. 限制

- 仅 1 主 1 从，无 quorum/自动选主；
- 同步复制吞吐低；
- 未验证复制期间 trim；
- `--broker-name` 不支持（slave 使用默认）。
