# R20 · Broker 锁与写路径优化

> 日期：2026-11-25  
> 状态：**topic 读优先、队列锁按 queue 拆分、CommitLog 单次写**；功能冒烟通过，性能 6.0–6.5k TPS。

---

## 1. 改动

### 1.1 topic map 读优先

旧实现每次 append 都对 `topics` 取写锁，即使 topic 已存在。  
现改为：

1. 先 `RwLock::read()` 查 topic；
2. 只有不存在时才 `write()` 插入。

### 1.2 分区队列锁

`TopicState.queues` 从 `Vec<QueueState>` 改为 `Vec<RwLock<QueueState>>`：

- append 只锁目标 queue；
- read/maxOffset 只读目标 queue；
- 不同 queue 的并发写入不再互相阻塞。

### 1.3 CommitLog 单次写

`CommitLog::append` 原来每条消息 4 次 `write_all`（magic/len/crc/payload），  
现合并为一个 buffer；Windows 使用 `seek_write` 定位写，减少 seek 系统调用。

### 1.4 避免重复 encode

`append` 已 encode 一次 payload，CQ 条目直接复用其长度，不再二次 `msg.encode()`。

## 2. 验证

```text
RESULT received=15 asyncOk=5 send=true
TX RESULT commit=1 rollback=0
```

性能（8 线程 / 20k / 1 KiB）：

| 场景 | TPS |
|---|---|
| read-first | 5,714 |
| read-first 第二次 | 6,392 |
| + per-queue | 6,067 |
| + per-queue 第二次 | 6,208 |
| + 单次写 | 6,025 |
| + 单次写第二次 | 6,415 |
| + seek_write | 6,415 |
| + 复用 encode 长度 | 6,627 / 6,434 |
| 100k 消息 / 8 线程 | **7,717 / 9,347 / 9,032** |
| SEND 并发 spawn（已回退） | 5,801 |
| 历史最佳（同机） | 7,127 |

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       59 项通过
```

## 4. 结论

- 优化未降低功能正确性；
- 性能受宿主负载影响大，短跑 6k、100k 长跑可达 **9,347 TPS**；
- 仍低于 5 万目标，下一步需多连接 pipeline / NVMe / 批量提交。

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`
- `crates/mq-store/src/lib.rs`
- `docs/rounds/R04-perf.md`（历史基线）
