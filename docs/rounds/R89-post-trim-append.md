# R89 · 段回收后继续写入

> 日期：2026-11-25  
> 结果：CommitLog 段回收/重启后继续写入 100 条，offset 继续增长，仍保留 4,000 条。

---

## 1. 验证

```text
同 store（已 trim、base≈33k）
发送 MQ_TRIM 100 条：0 失败
/stats：
  messages=4000
  commitlog_bytes=16,333,056
RANGE queue0：min=37,669 max=38,169
PULL_ONLY 4000/4000
```

## 2. 说明

- 新消息 offset 接续 trim 前的全局 offset；
- 旧段删除后 `append` 使用 `.meta` 中的 base 继续；
- Retention 再次保留 500/队列；
- 拉取偏移连续。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       77 项通过
```

## 4. 限制

- 只验证单分片/单 broker；
- 未验证多副本下 trim + 同步复制；
- meta 非原子写。
