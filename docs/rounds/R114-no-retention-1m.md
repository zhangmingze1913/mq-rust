# R114 · 1M 无 Retention 对照

> 日期：2026-11-25  
> 结果：1M 全量留在内存时 **4,073 TPS、32 失败**；Retention 500/队列时 8,438 TPS、0 失败。

---

## 1. 数据

```text
1,000,000 × 256B / 8 线程
无 retention：
  Send TPS 4,073
  Send Failed 32
  messages=1,000,000
  body_bytes=256,000,000
  commitlog_bytes=418,000,000
  Max RT 1,934ms

retention 500/队列（R94）：
  Send TPS 8,438
  Send Failed 0
  messages=4,000
```

## 2. 分析

- `queue.messages` 保存完整 `StoredMessage`（含 body），1M 时内存与分配压力大；
- 每消息多次 clone/encode（队列、索引、复制、审计）；
- 全量内存导致长尾与超时失败；
- Retention 限制内存后吞吐反而更高。

## 3. 结论

- 长跑必须配置 Retention；
- 若要无界 1M 且接近原生 15k，需要 **metadata-only 队列 + 读时从 CommitLog 取 body**；
- 当前 0.9.0-rc.1 不建议无限保留。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 单次采样；
- 未做内存 profile；
- 未验证 500k/2M。
