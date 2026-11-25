# R93 · 非对齐 Retention 消除 CQ 全量修复

> 日期：2026-11-25  
> 结果：非对齐 drop 改为 CQ 自身重写；缓存句柄复用 + CQ 写在队列锁内，200k 压测 **0 次 repair**、0 失败。

---

## 1. 改动

- `ConsumeQueue::drop_prefix` 非对齐：读出剩余条目 → `truncate` → 重新 append；
- `BrokerStore::drop_cq_prefix` 改用 `self.cq` 缓存句柄，避免保留线程与 append 各自打开文件；
- `append_cq` 移到队列写锁内，保证消息入队与 CQ 追加原子。

## 2. 验证

```text
200k / 256B / 8 线程，CQ=1000，retain=500/队列
Send TPS 5,725
Max RT 314ms
Send Failed 0
repair consume queue 次数 = 0
messages=4000, dropped=196000, commitlog=17,291,450
PULL_ONLY 4000/4000
重启后 PULL_ONLY 4000/4000
```

单测：

```text
consumequeue_drop_prefix_unaligned ... ok
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 非对齐 drop 仍需重写剩余条目（O(remaining)）；
- 1M 长跑 TPS 仍低于 5 万目标；
- 未验证多副本下该路径。
