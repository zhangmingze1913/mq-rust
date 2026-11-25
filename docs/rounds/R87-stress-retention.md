# R87 · 200k 持续写入 + Retention 压力

> 日期：2026-11-25  
> 结果：200k 消息 5,896 TPS、0 失败；Retention 丢弃 196k、保留 4,000；重启后 4,000/4,000。

---

## 1. 参数

```text
--commitlog-segment-bytes 64MiB
--cq-segment-entries 1000
--retain-messages 500 --retain-interval-ms 1000
消息 256B / 8 线程 / 200,000 条
```

## 2. 结果

```text
发送：200,000，TPS 5,896，0 失败
/stats：
  messages=4000
  retained_dropped=196000
  commitlog_bytes=83,800,000
  body_bytes=1,024,000
PULL_ONLY received=4000 expected=4000
重启后：
  messages=4000, retained_dropped=196000
  PULL_ONLY 4000/4000
```

## 3. 说明

- 每队列保留 500，8 队列共 4,000；
- CQ 段大小 1,000，Retention 对齐/非对齐混合路径均正常；
- CommitLog 分 2 段（64MiB + 余量），重启跨段恢复；
- **Retention 不删除 CommitLog**：磁盘仍 83.8MB，重启 replay 200k 后再丢弃。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       75 项通过
```

## 5. 限制

- CommitLog 无段删除/回收；
- 重启 replay 全量历史；
- 长期运行需 CommitLog 清理。
