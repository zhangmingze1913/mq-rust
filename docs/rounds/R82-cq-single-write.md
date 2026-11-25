# R82 · CQ 单次写优化

> 日期：2026-11-25  
> 结果：CQ 20 字节条目由 3 次 write 合并为 1 次；100k 9,196/9,254 TPS，处于原区间。

---

## 1. 改动

`ConsumeQueue::append` 原先：

```text
seek end
write 8B offset
write 4B size
write 8B tag_hash
```

现在：

```text
seek end
write 20B entry
```

## 2. 数据

```text
100k / 8 线程 / 1 KiB
第 1 次：9,196 TPS
第 2 次：9,254 TPS
历史最佳：9,347 TPS
```

差异在系统噪声范围内，无回归。

## 3. 结论

- 减少 2 次 syscall/消息，代码更简洁；
- 当前瓶颈仍是 CommitLog/HDD/单锁，未显著提升 TPS；
- 保留该改动。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```
