# R122 · CQ 段大小调优（负结果）

> 日期：2026-11-25  
> 结果：CQ 段 125 条反而降到 **2,418 TPS、40 失败**；CQ 1000 仍最佳。

---

## 1. 数据

```text
1M × 256B / 8 线程
retain=500/队列，interval=1s

CQ segment=1000：8,438 TPS，0 失败（R94）
CQ segment=125 ：2,418 TPS，40 失败
```

## 2. 分析

- 125 条/段产生约 8,000 个 CQ 文件；
- 每秒每队列删除 4 个小段，文件创建/删除/重命名开销大；
- 小文件在 HDD 上碎片化严重；
- 对齐回收的收益被文件系统开销抵消。

## 3. 结论

- 暂停 CQ 段调优；
- 保持 `--cq-segment-entries 1000`；
- 若换 SSD，可再测 250/500/2000。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```
