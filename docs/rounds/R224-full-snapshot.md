# R224 · 完整状态机快照模式

> 日期：2026-11-27  
> 结果：新增 `MQ_SNAPSHOT_FULL=1`，快照保存全部消息；单测通过。

---

## 1. 实现

- 默认仍有界尾部 1000 条；
- `MQ_SNAPSHOT_FULL=1` 时 `build_snapshot` 序列化全部消息；
- install/restart 恢复全部消息。

## 2. 测试

```text
snapshot_full_mode_keeps_all_messages ... ok
```

- 1500 条消息；
- full 模式快照后重开仍为 1500；
- 默认模式仍为 1000。

## 3. 用法

```text
MQ_SNAPSHOT_FULL=1 mq-broker ...
```

## 4. 边界

- 全量快照暂停时间随消息数增长；
- 动态加节点 API 仍未实现；
- 快照与 CommitLog 的一致性未做 1M 验证。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
