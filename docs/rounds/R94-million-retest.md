# R94 · 1M 复测：CQ repair 消除后

> 日期：2026-11-25  
> 结果：1M 条 **8,438 TPS、0 失败、0 次 CQ repair**；对比 R92 的 4,572 TPS / 16 失败。

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
Send TPS: 8,438
Max RT: 2,265 ms
Average RT: 0.950 ms
Send Failed: 0
Response Failed: 0

/stats：
  messages=4000
  retained_dropped=996000
  commitlog_bytes=18,348,022
  body_bytes=1,024,000

repair consume queue 次数 = 0
PULL_ONLY 4000/4000
重启后 PULL_ONLY 4000/4000
```

## 3. 对比

| 版本 | TPS | 失败 | repair |
|---|---|---|---|
| R92（全量 repair） | 4,572 | 16 | 多次 |
| R94（CQ 自身重写） | **8,438** | **0** | **0** |

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 5 万 TPS 未达成；
- Max RT 2.2s 仍有长尾；
- 仅单 broker / 单分片。
