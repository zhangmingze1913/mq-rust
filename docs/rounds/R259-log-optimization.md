# R259 · 热路径日志优化

> 日期：2026-11-28  
> 结果：批量 appended 日志降为 debug；300s 231,424/14,848；RUST_LOG=warn 后 372,224/9,728、p99 3.67s。

---

## 1. 变更

- `batch message appended` 从 INFO 降为 DEBUG；
- 其他热路径日志保持 debug/按次。

## 2. 300s（DEBUG 日志可用）

```text
2P/4C 300s: sent=231,424 failed=14,848
p50=199ms p99=17.2s
```

## 3. 300s（RUST_LOG=warn）

```text
2P/4C 300s: sent=372,224 failed=9,728
p50=190ms p99=3.67s
```

## 4. 结论

- 日志是 300s 尾段的重要开销；
- 默认 INFO 下每批一行日志，长期压测放大；
- 生产建议 `RUST_LOG=warn`，批量日志降级；
- 最佳 sustained：372k/300s，p99 3.67s，失败 2.6%。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（94 项）
```
