# R137 · 轨迹文件轮转

> 日期：2026-11-25  
> 结果：`--trace-max-bytes`（默认 64MiB）触发 JSONL 轮转，保留一个 `.1` 备份；测试 82 项。

---

## 1. 实现

- `TraceLog::open_with_limit(capacity, path, max_bytes)`；
- 每条记录后检查文件大小，超过阈值：
  - 删除旧 `.1`；
  - 当前文件重命名为 `<name>.1`；
  - 下一条记录重建当前文件；
- 启动时先读 `.1` 再读当前文件，受 capacity 限制；
- CLI `--trace-max-bytes`。

## 2. 测试

```text
trace_rotates_file ... ok
```

```text
max_bytes=1
record m1 -> 当前文件不存在，.1 存在
reopen -> by_msg_id(m1)=1
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 4. 限制

- 只保留一个备份，更早历史丢弃；
- 轮转为 rename，未做跨盘/权限失败重试；
- 内存查询仍只含最近 capacity 条。
