# R156 · 批量 CommitLog 合并（负结果）

> 日期：2026-11-25  
> 结果：batch256 合并提交未提升，约 31–33k msg/s，低于原 37,095 峰值；代码已回退。

---

## 1. 尝试

- `CommitWriter::submit_many`：一次提交整批 job，让 writer 线程一次合并 64 条；
- 批量 handler 收集消息后一次提交；
- 改动仅在 store/server，未改协议。

## 2. 结果

```text
batch256 -s128 -w8
Send TPS 97  MPS 24957
Send TPS 127 MPS 32739
Send TPS 130 MPS 33308
Send TPS 121 MPS 31037
```

对比原 batch256 峰值 37,095。

## 3. 分析

- CommitLog 写并非主要瓶颈；
- 每消息的 queue 锁、CQ 追加、index/dedup 更新与 CPU 序列化占用更高；
- 合并提交还增加批内等待，收益为负。

## 4. 处理

- 批量 handler 恢复逐条 append；
- 取消未使用的 `append_batch/submit_many`；
- 82 项测试通过。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
git status                   干净
```

## 6. 结论

5 万仍需硬件/集群；单机 4 线程下继续微调批量写收益有限。
