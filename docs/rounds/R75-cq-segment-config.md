# R75 · CQ 段大小可配置 + 跨段验证

> 日期：2026-11-25  
> 结果：`--cq-segment-entries` 可配置；段大小 4 时发送 60 条产生 `.1/.2` 段，PullOnly 跨段拉取 60/60。

---

## 1. 机制

- `ConsumeQueue::open_with_segment(path, entries)`；
- `open()` 默认 100,000；
- Broker `--cq-segment-entries` 传播到 append/repair/read；
- 段文件 `<queue>.cq`、`.cq.1`、`.cq.2`。

## 2. 验证

Broker 启动：

```text
--cq-segment-entries 4
```

发送 60 条（8 队列）：

```text
0.cq     80B
0.cq.1   20B
2.cq     80B
2.cq.1   80B
2.cq.2   60B
...
```

跨段拉取：

```text
PULL_ONLY received=60 expected=60
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       70 项通过
```

## 4. 限制

- 段大小启动时设置，运行期不可变；
- Retention 仍全量重写；
- 每次 open 扫描段数。

## 5. 涉及文件

- `crates/mq-store/src/lib.rs`
- `crates/mq-broker/src/store.rs`
- `crates/mq-broker/src/main.rs`
