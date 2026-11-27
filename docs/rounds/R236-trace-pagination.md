# R236 · 轨迹按时间分页

> 日期：2026-11-27  
> 结果：`/traces` 支持 `from/to/offset/limit`；新增 `/traces/page` 返回 total/offset/limit/events；单测通过。

---

## 1. 变更

- `TraceLog::page(topic, from, to, offset, limit)`；
- 过滤后按时间倒序，支持 offset/limit；
- `/traces` 兼容原有数组返回；
- `/traces/page` 返回分页对象。

## 2. 示例

```text
GET /traces?topic=T&from=1700000000000&to=1700000001000&offset=0&limit=20
GET /traces/page?offset=20&limit=20
```

## 3. 测试

```text
trace_page_filters_and_offsets ... ok
```

- 5 条记录 offset=1 limit=2 返回 m3/m2；
- from 未来时间返回 0 条。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（91 项）
```
