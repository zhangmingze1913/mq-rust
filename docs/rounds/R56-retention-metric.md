# R56 · Retention 丢弃计数指标

> 日期：2026-11-25  
> 结果：新增 `mq_broker_retained_dropped_total`，验证 15 条 / 保留 1 条/队列 → 丢弃 7、保留 8。

---

## 1. 验证

```text
--retain-messages 1 --retain-interval-ms 1000
发送 15 条 → RESULT received=8（拉取仅剩 8）
mq_broker_store_messages 8
mq_broker_retained_dropped_total 7
```

## 2. 实现

| 项 | 说明 |
|---|---|
| `BrokerStore.retained_dropped` | AtomicU64 |
| `retain` | 实际丢弃数累加 |
| `/metrics` | `# TYPE ... counter` |
| 刷新 | 5s 后台任务同步 |

## 3. 限制

- 计数仅内存，Broker 重启归零；
- 未按 topic/queue 维度拆分；
- Retention 仍会全量重写 CQ。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`：计数
- `crates/mq-broker/src/metrics.rs`：指标
- `crates/mq-broker/src/main.rs`：刷新
