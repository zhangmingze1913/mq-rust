# R22 · 消费位点持久化

> 日期：2026-11-25  
> 状态：**Broker 消费位点写入 `offsets` 日志，重启后恢复**。

---

## 1. 改动

- `BrokerStore` 增加 `offset_log`（CommitLog）；
- `update_offset` 同步追加 `(group, topic, queue_id, offset)`；
- `open` 时扫描 `offsets`，按记录顺序重建位点（后写覆盖前写）；
- `--metadata-compact-ms`（默认 60000）周期压缩 `index`/`offsets`；
- 启动时也会压缩一次。

## 2. 测试

```text
test store::tests::offsets_survive_reopen ... ok
test store::tests::metadata_logs_compact_on_open ... ok
```

- `g1/orders/0 = 5`；
- 关闭/重新打开；
- 查询仍为 5；
- 50 次重复更新后重启，`offsets` 文件被压缩，值仍为 49；
- 运行期 `--metadata-compact-ms 1000` 下持续写入/拉取正常：`15/15`。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
```

## 4. 限制

| 限制 | 计划 |
|---|---|
| 位点日志无压缩 | ✅ 启动 + 周期压缩 |
| 未做位点批量写 | 后续 |
| 位点未与 CQ 校验 | 后续 |
| 无多副本位点 | Raft/HA |

## 5. 涉及文件

- `crates/mq-broker/src/store.rs`
