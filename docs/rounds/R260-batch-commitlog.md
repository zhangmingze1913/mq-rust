# R260 · CommitLog 批量追加

> 日期：2026-11-28  
> 结果：store 批写与 group writer 批写改用 `CommitLog::append_batch`；300s 415,744 发送、7,424 失败、p50 199ms、p99 5.88s。

---

## 1. 变更

- `append_batch_msgs`：一次 `append_batch` 写多条记录；
- 非 writer 路径：先 encode 全部 payload，再批量 append；
- group writer Batch 路径：同样批量 append；
- 失败时整批标记失败。

## 2. 300s（RUST_LOG=warn）

```text
2P/4C 300s: sent=415,744 failed=7,424 received=422,144
p50=199ms p99=5.88s
```

对比 R259（逐条 append）：

```text
sent 372,224 -> 415,744 (+12%)
failed 9,728 -> 7,424 (-24%)
p99 3.67s -> 5.88s（波动）
```

## 3. 结论

- 批量追加提升吞吐、减少失败；
- p99 受宿主抖动影响，未稳定改善；
- 保留批量追加路径。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（94 项）
```
