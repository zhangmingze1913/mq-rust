# R86 · CommitLog 分段

> 日期：2026-11-25  
> 结果：CommitLog 支持分段（默认 256MB，可配置）；Broker 256B/段测试产生 15 个段，重启后仍拉取 15/15。

---

## 1. 机制

- 段文件：`commitlog-0`、`commitlog-0.1`、`.2` …；
- `--commitlog-segment-bytes`（默认 268435456）；
- `append` 当前段不足时自动开新段；
- `read_at`/`scan`/`recover` 跨段；
- `recover` 遇到损坏截断当前段并删除后续段；
- 半消息/重试/延时/索引/位点日志同样使用分段。

## 2. 验证

单测：

```text
test commitlog_rotates_segments ... ok
```

- 64B/段，10 条 20B 记录；
- 生成 `commitlog.1`；
- 逐条 read_at、scan=10、重开 scan=10。

Broker：

```text
--commitlog-segment-bytes 256
15 条 → commitlog-0 … commitlog-0.14
PULL_ONLY 15/15
重启后 PULL_ONLY 15/15
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       75 项通过
```

## 4. 限制

- 无段删除/清理，磁盘不会回收；
- 段大小启动时固定；
- 恢复只保留到首个损坏段；
- 索引/位点日志分段后仍全量扫描。
