# R226 · 在线 CommitLog 压缩端点

> 日期：2026-11-27  
> 结果：`POST /admin /compact` 可在运行中触发精确压缩；本机测试返回 `{"compacted":true}`。

---

## 1. 变更

- `BrokerStore::compact_commitlogs_exact` 改为 `&self`，通过 `Mutex<CommitLog>` 替换；
- 新增 `compact_now()`：精确压缩 + 重建 CQ；
- Admin 新增 `POST /compact`。

## 2. 实测

```text
mqadmin sendMessage -> SEND_OK
POST /compact -> {"compacted":true}
/stats messages=1，commitlog 168B
```

无浪费时压缩后文件仍为 168B；有 retention 丢弃时可释放。

## 3. 限制

- group-writer 模式仍跳过；
- 压缩会阻塞该 broker 的写入；
- 未做压缩期间并发写测试。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
