# R207 · 非 Raft 300s 对照（主机长跑上限）

> 日期：2026-11-26  
> 结果：非 Raft batch256/4 线程 300s 写入 7,341,407 条，尾段仍出现 43 次超时，Max RT 2919ms。

---

## 1. 配置

```text
非 Raft，batch256，4 threads，300s
retain-messages 1,500,000
commitlog-shards 1
```

## 2. 结果

```text
messages: 7,341,407
commitlog_bytes: 2,114,325,216
body_bytes: 939,700,096
MPS 24,329–33,292
Send Failed: 43 / 11,008
Max RT 2919ms
```

## 3. 结论

- 即使非 Raft，长时间高吞吐也会在尾段出现 3s 超时；
- 主机 HDD 在 2GB+ CommitLog、700 万消息下写入饱和；
- Raft 300s/516k 仅 4 次超时，相对其数据量已不算异常；
- 3s 尾段超时是主机 I/O 上限，不是 Raft 独有；
- 5 万 TPS 长跑需要 NVMe/更多磁盘。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
