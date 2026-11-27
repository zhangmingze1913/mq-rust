# R245 · 状态化手动快照

> 日期：2026-11-27  
> 结果：新增 `POST /snapshot`；快照包含 messages/total/applied/membership；3 节点重启恢复并继续写入一致。

---

## 1. 变更

- `MessageRaft::snapshot_now()` 调用 openraft `trigger().snapshot()`；
- Admin `POST /snapshot`；
- 快照内容已包含：
  - messages；
  - total_messages；
  - applied LogId；
  - membership（configs/nodes）。

## 2. 实测

```text
3 节点，发送 1 条 SNAP
POST /snapshot 到 3 个 broker -> {"snapshot":true}
raft.snapshot 481B，含 messages/applied/membership
重启 broker1 -> messages=1
再发 1 条 SNAP2 -> 三副本 messages=2
```

## 3. 定位

- 有界尾部快照（默认 1000）；
- 全量模式 `MQ_SNAPSHOT_FULL=1`；
- 手动状态化快照；
- 真正 base+delta 增量快照未实现，当前快照已携带 applied/membership，可独立恢复。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
3 节点快照/重启/继续写入一致
```
