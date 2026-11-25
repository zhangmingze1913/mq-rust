# R130 · 轨迹持久化

> 日期：2026-11-25  
> 结果：`--trace-path` 将 send/pull 轨迹写入 JSONL，重启后 `/trace/{msgId}` 仍可查；Rust 测试 81 项。

---

## 1. 实现

- `TraceLog::open(capacity, path)`：
  - 启动时读取 JSONL，保留最近 capacity 条；
  - 每条新事件追加一行 JSON；
- CLI `--trace-path`；
- 内存仍有界，磁盘文件持续增长（未轮转）。

## 2. 验证

```text
启动：--trace-path .../trace.jsonl
AclSmoke 发送 msgId=7F0000012B44...
PullOnly 消费 1 条

BEFORE 重启：
[{send,...},{pull,...}]

kill + 同 store/trace-path 重启
AFTER：
[{send,...},{pull,...}]
```

单测：

```text
trace_persists_and_reloads ... ok
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       81 项通过
```

## 4. 限制

- JSONL 无轮转/压缩；
- 无按 topic/时间查询；
- 多副本各写各的轨迹。
