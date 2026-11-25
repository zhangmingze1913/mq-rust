# R36 · 批量发送性能与批量落盘实验

> 日期：2026-11-25  
> 结果：批量 16 条 13.5k–19.2k msg/s；64 条 21.7k–25.0k msg/s；  
> `append_batch` 服务端改造导致挂起/失败，**已回退**。

---

## 1. 基线

```text
BatchProducer -w 8 -s 1024
```

| batchSize | 批次 TPS | 消息 MPS |
|---|---|---|
| 16 | 846–1,202 | 13,549–19,243 |
| 64 | 338–390 | 21,682–24,982 |

稳定后 `Send Failed=0`；早期统计行的失败为客户端 warm-up。

## 2. 批量落盘实验（失败，已回退）

尝试：

- `BrokerStore::append_batch`：CommitLog 单锁 + `append_batch`；
- CQ `append_many`、索引批量写。

结果：

- 64 条批量首轮 23,286 msg/s，随后出现 **8 批/512 条失败**、延迟 2.7s；
- 回退前的 BatchSmoke 出现发送挂起；
- **全部回退**，恢复逐条 `store.append`。

回退后：

```text
BATCH RESULT received=5
```

## 3. 对比

| 场景 | 结果 |
|---|---|
| 单条 100k 长跑 | 9,347 msg/s |
| 批量 64 条基线 | **24,982 msg/s** |
| 5 万目标 | 未达成 |

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
BatchSmoke                    5/5
```

## 5. 结论

- 批量基线可用且无失败；
- 服务端批量落盘改造在 64 条高压下不稳定，已回退；
- 后续若要再做，需要先定位批量路径的阻塞/超时根因；
- 5 万 TPS 未达成。
