# R240 · group-writer 在线压缩

> 日期：2026-11-27  
> 结果：group-writer 模式也能 `POST /compact`；CommitWriter 新增 Rebuild，truncate 后重放存活消息；单测通过。

---

## 1. 变更

- `WriteCmd::Rebuild { msgs, reply }`；
- `CommitWriter::rebuild`；
- writer 收到 Rebuild 时 `log.truncate()`，按存活消息重写并分配新 offset；
- `compact_commitlogs_exact` 在 writer 模式走该路径；
- 非 group 模式保留原文件替换路径。

## 2. 测试

```text
group_writer_commitlog_compaction_keeps_survivors ... ok
4 条 retain 1，压缩后 commitlog < 4KB，读取 1 条
```

## 3. 用法

```text
POST /compact
```

group-writer 与非 group-writer 均可用。

## 4. 边界

- 压缩期间 writer 阻塞；
- 未做并发写入压测；
- 多 shard group 模式仍为单 shard。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
```
