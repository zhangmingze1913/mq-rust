# R39 · 队列数调优

> 日期：2026-11-25  
> 结果：8 队列仍最佳；4/16 队列均明显下降。

---

## 1. 测试

100k / 8 线程 / 1 KiB / 单 CommitLog：

| queues | TPS |
|---|---|
| 4 | 4,112 |
| 8（历史最佳） | **9,347** |
| 16 | 4,399 |

## 2. 分析

- 4 队列：分区队列锁竞争更高，producer 并发分散不足；
- 16 队列：更多 CQ 文件/队列状态，缓存与锁开销上升；
- 8 队列在当前 4 核宿主上最平衡。

## 3. 结论

默认保持 `--queues 8`；不调整默认值。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       62 项通过
```
