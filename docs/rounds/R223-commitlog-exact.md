# R223 · CommitLog 精确回收（离线压缩）

> 日期：2026-11-27  
> 结果：新增 `MQ_COMPACT_COMMITLOG=1` 启动压缩；按存活消息重写 CommitLog 并重建 CQ，测试通过。

---

## 1. 实现

- 启动时读取 `MQ_COMPACT_COMMITLOG=1`；
- 按 shard 收集存活消息（按旧 physical_offset 排序）；
- 写入 `commitlog-N.compact`，再原子替换旧文件；
- 更新每条消息的 `physical_offset`；
- 删除并重建 CQ，保证 offset 一致；
- 仅支持非 group-writer 模式（当前单/多 shard 直接日志）。

## 2. 测试

```text
exact_commitlog_compaction_keeps_survivors_readable ... ok
```

- 4 条消息 retain 1 条；
- 压缩后文件 < 4KB；
- 读取仍返回 1 条，offset 有效。

## 3. 用法

```text
MQ_COMPACT_COMMITLOG=1 mq-broker ...
```

重启/启动时执行一次精确压缩。

## 4. 边界

- 不是在线持续压缩；
- group-writer 模式跳过；
- 首次启动大 store 会阻塞启动；
- 需配合 retention 配置。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（89 项）
```
