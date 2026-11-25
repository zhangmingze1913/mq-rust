# R115 · 无副本时跳过 encode 优化

> 日期：2026-11-25  
> 结果：无 replica 时不再编码复制 payload；1M 无 Retention 从 4,073 TPS/32 失败 提升到 4,481 TPS/0 失败。

---

## 1. 改动

- 新增 `replica_active` 标志；
- `add_replica` 首次连接后置 true；
- `append` 仅在 `replica_active` 时执行 `msg.encode()` + `broadcast_replica`；
- 无副本单机场景省去每消息一次 body 复制/编码。

## 2. 验证

```text
1,000,000 × 256B / 8 线程，无 retention

优化前：4,073 TPS，32 失败
优化后：4,481 TPS，0 失败
messages=1,000,000
```

提升约 10%，并消除本次发送失败；仍低于原生 15,465。

## 3. 回退实验

- 尝试移除 `index` 日志写入：1M 反而 2,683 TPS；
- 已完整回退，保留原索引日志/元数据压缩逻辑。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 优化只影响无副本路径；
- 无界 1M 仍受内存队列限制；
- 原生 15.5k、目标 50k 均未达。
