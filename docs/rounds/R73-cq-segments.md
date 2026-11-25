# R73 · ConsumeQueue 分段

> 日期：2026-11-25  
> 结果：CQ 按 100k 条目分段（测试环境 4），跨段读写/截断正确；SDK 15/15、拉取 15/15。

---

## 1. 机制

- 段文件：`<queue>.cq`、`<queue>.cq.1`、`.2` …；
- 每段最多 `CQ_SEGMENT_ENTRIES`（生产 100,000，测试 4）；
- `open` 顺序扫描段，累计条目；
- `append` 自动创建新段；
- `read` 跨段定位；
- `truncate` 清空并删除 `.1+` 段；
- `sync` 同步所有段。

## 2. 验证

单测：

```text
test consumequeue_rotates_segments ... ok
```

- 10 条（测试段大小 4）→ 3 个文件；
- `read(3,5)` 跨段返回 offset 3..7；
- `truncate` 后 `.1` 被删除，重开长度 0。

SDK：

```text
RESULT received=15 asyncOk=5 send=true
PULL_ONLY received=15 expected=15
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       70 项通过
```

## 4. 限制

- 段大小固定，不可配置；
- Retention 仍全量重写，未按段删除；
- 每次 `ConsumeQueue::open` 扫描所有段；
- 无段级 CRC/损坏隔离。

## 5. 涉及文件

- `crates/mq-store/src/lib.rs`
