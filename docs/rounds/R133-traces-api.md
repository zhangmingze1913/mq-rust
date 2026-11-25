# R133 · /traces 轨迹查询

> 日期：2026-11-25  
> 结果：`GET /traces?topic=&limit=` 返回最近轨迹，支持 topic 过滤。

---

## 1. 实现

- `TraceLog::recent(topic, limit)`：按 topic 过滤，保留最近 limit 条；
- Admin：
  - `GET /traces?limit=N`；
  - `GET /traces?topic=MQ_TRACES&limit=N`；
  - limit 1–500，默认 50；
  - 返回 JSON 数组。

## 2. 验证

```text
GET /traces?limit=5
[{"event":"send","msg_id":"7F...","topic":"MQ_TRACES","queue_id":0,"queue_offset":0,"timestamp":...}]

GET /traces?topic=MQ_TRACES&limit=5
同上
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       81 项通过
```

## 4. 限制

- 仅内存最近 capacity 条；
- 无时间范围/分页；
- 持久化历史需读 JSONL 文件。
