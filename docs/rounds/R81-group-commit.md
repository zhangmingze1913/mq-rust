# R81 · Group Commit 性能验证（负结果）

> 日期：2026-11-25  
> 结果：`--group-commit` 100k 仅 **4,155 TPS**，明显低于默认 9,347，不采用。

---

## 1. 数据

```text
100k / 8 线程 / 1 KiB
默认直接 append：9,347 TPS（历史最佳）
--group-commit：4,155 TPS
```

## 2. 分析

- 当前 Group Commit 后台 writer 每次仅处理单条提交，额外线程/通道开销；
- 未形成真正的批量 fsync/批量写；
- 在 HDD 上无收益，反而增加延迟。

## 3. 结论

- 默认路径保持直接 append；
- `--group-commit` 保留为实验开关，不推荐生产；
- 若后续实现真正批量提交（聚合多请求 + 单次写/fsync）再评估。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```
