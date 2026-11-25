# R92 · 1,000,000 条持续写入

> 日期：2026-11-25  
> 结果：1M 条 256B 写入 **4,572 TPS**，16 条发送失败（0.0016%）；Retention 丢弃 996k，保留 4,000。

---

## 1. 参数

```text
--commitlog-segment-bytes 64MiB
--cq-segment-entries 1000
--retain-messages 500 --retain-interval-ms 1000
1,000,000 × 256B / 8 线程
```

## 2. 结果

```text
Send Total: 1,000,000
Send TPS: 4,572
Max RT: 1,195 ms
Average RT: 1.728 ms
Send Failed: 16
Response Failed: 0

/stats：
  messages=4000
  retained_dropped=996000
  commitlog_bytes=17,346,840
  body_bytes=1,024,000
```

## 3. 现象

- Retention + CQ 修复（非对齐 drop 回退 repair）周期性停顿；
- 1M 过程中 Max RT 1,195ms；
- 16 条发送失败，broker 无崩溃；
- CommitLog 段回收后仅 17.3MB，磁盘有界；
- TPS 从 9k 降到 4.6k，说明 Retention/repair 是长跑瓶颈。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       78 项通过
```

## 5. 限制

- 16 条失败需客户端重试兜底；
- 非对齐 Retention 触发全量 CQ 重写；
- 5 万 TPS 未达成。
