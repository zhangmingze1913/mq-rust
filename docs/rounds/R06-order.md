# R06 · 顺序消息（队列级）

> 日期：2026-11-24  
> 状态：**同一队列内严格有序已打通**。跨队列/全局顺序、故障切换保序待补。

---

## 1. 结果

```text
ORDER RESULT done=true count=50 ordered=true
```

- Producer 用 `MessageQueueSelector` 固定选择 `mqs.get(0)`；
- Consumer 用 `MessageListenerOrderly`；
- 50 条消息按 `order-0..order-49` 顺序消费。

---

## 2. 实现要点

- Broker 的 CommitLog/消费队列按 `(topic, queue_id)` 追加，天然保持队列内顺序；
- Pull 返回 `queue_offset` 递增；
- 客户端 `ConsumeMessageOrderlyService` 保证同一队列单线程消费；
- Broker 未做额外全局锁，顺序语义等价 RocketMQ 4.x 的队列级顺序。

---

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       45 项通过
```

---

## 4. 已知限制

| 限制 | 计划 |
|---|---|
| 仅队列级顺序 | 全局顺序需要单队列/单分区 |
| 主从切换后的顺序连续性 | R6.1 HA |
| 顺序消息与事务消息组合 | 后续 |
| 无顺序消费堆积告警 | 后续 metrics |

---

## 5. 下一轮

1. 顺序消费堆积指标。
2. 主从/HA 下的 offset 连续性。
3. TC Raft。
4. group commit 性能优化。
