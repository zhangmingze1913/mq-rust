# R206 · ConsumeQueue 写缓冲（负结果，已回退）

> 日期：2026-11-26  
> 结果：CQ 写缓冲在 300s 三副本负载下超时更多，已回退。

---

## 1. 尝试

- `ConsumeQueue` 增加 64 条（1280B）写缓冲；
- 段切换/read/sync 前 flush；
- 降低每消息 seek+write 次数。

## 2. 结果

```text
3 副本 Raft，batch256，4 threads，300s
messages 512,256（三副本一致）
Send Failed 73 / 18,688
Max RT 2674ms
```

对比同一 300s 配置（R205）：

```text
R205: 516,608 / 4 失败
R206: 512,256 / 73 失败
```

## 3. 结论

- 写缓冲未改善，超时反而增加；
- 可能与 flush/read 锁竞争有关；
- 已回退 `mq-store`；
- 保留 R205 快照阈值提升。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
