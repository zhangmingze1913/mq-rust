# R80 · 消费吞吐基准

> 日期：2026-11-25  
> 结果：100k backlog 用 20 线程消费，TPS **11,130**，FAIL 0；生产端 8,485 TPS。

---

## 1. 数据

```text
Produce 100k / 8 线程 / 1 KiB：8,485 TPS，0 失败
Consume 20 线程：11,130 TPS，0 失败
```

- 消费 RT 很大（18–24s），因为消息在生产后等待消费；
- 第二个 10s 窗口 TPS=0，说明 backlog 已清空。

## 2. 对比

| 场景 | TPS |
|---|---|
| 生产 100k | 8,485–9,347 |
| 消费 20 线程 | **11,130** |
| 5 万目标 | 未达成 |

## 3. 结论

- 消费侧吞吐高于生产侧，当前瓶颈仍在生产 CommitLog/CQ 写入；
- 消费无 FAIL；
- 5 万 TPS 仍未达成。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```
