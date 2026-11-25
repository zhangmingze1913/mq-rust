# R38 · ConsumeQueue 启动修复

> 日期：2026-11-25  
> 结果：启动时校验 CQ 条目数与内存队列长度，不一致则重写 CQ。

---

## 1. 机制

- `BrokerStore::open` 完成 CommitLog replay 后调用 `repair_cq`；
- 对每个 topic/queue：
  - CQ 条目数 == 消息数 → 跳过；
  - 否则 `ConsumeQueue::truncate` 后按消息 physical_offset/size 重写。

覆盖：崩溃导致 CQ 多写/少写/断层。

## 2. 测试

```text
test store::tests::consume_queue_repair_on_open ... ok
```

- 正常写 2 条；
- 人为向 CQ 追加 1 条（共 3）；
- 重启后修复为 2 条，读回 2 条。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       62 项通过
```

## 4. 限制

- 修复为启动全量校验，消息量大时启动变慢；
- CQ 轮转/清理仍未实现。

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`：`repair_cq`
- `crates/mq-store/src/lib.rs`：`ConsumeQueue::truncate`
