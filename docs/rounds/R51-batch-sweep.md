# R51 · 批量大小扫描

> 日期：2026-11-25  
> 结果：batchSize=64 最优（~25k msg/s）；1024 因延迟上升降到 17–21k。

---

## 1. 数据

| batchSize | 批次 TPS | 消息 MPS | 平均消息延迟 |
|---|---|---|---|
| 16 | 846–1,202 | 13,549–19,243 | 0.54 ms |
| 64 | 338–390 | 21,682–24,982 | 0.30–0.37 ms |
| 1024 | 16–20 | 17,258–20,914 | 0.40–0.47 ms |

## 2. 结论

- 64 条/包在当前 Broker（逐条 append + CQ/索引）上最平衡；
- 1024 条/包单请求延迟 400–480 ms，批次 TPS 过低，整体 MPS 反而下降；
- 5 万 msg/s 仍未达到。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```
