# R74 · 分段 CQ 性能回归

> 日期：2026-11-25  
> 结果：CQ 分段后 100k 长跑 5,973 / 9,164 TPS，与分段前 9,347 区间一致；无回归。

---

## 1. 数据

```text
100k / 8 线程 / 1 KiB
第 1 次：5,973 TPS（系统负载）
第 2 次：9,164 TPS
```

分段前最佳：9,347 TPS。100k 消息未跨 100k 段，因此无额外段切换；`open` 扫描段数仍为 1。

## 2. 功能

```text
Compat 15/15
PullOnly 15/15
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       70 项通过
```

## 4. 结论

- 分段未引入可见性能回归；
- 5 万 TPS 仍未达成；
- 段切换（>100k/queue）未在性能测试中触发，仅单测覆盖。
