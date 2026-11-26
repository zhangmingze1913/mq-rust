# R210 · Raft 日志格式修复与 4 broker 验证

> 日期：2026-11-26  
> 结果：JSONL 中的 membership 记录无法反序列化导致 4 节点重启失败；改为 bincode 帧并支持旧 JSONL 迁移；4 节点初始收发通过。

---

## 1. 根因

4 节点 B 重启失败：

```text
StorageError: LogIndex(0) not found
```

`raft.log` 中 membership 记录：

```json
{"Membership":{"configs":[[1,2,3,4]],"nodes":{"1":null,...}}}
```

serde_json 反序列化 `nodes` 的 u64 key 报 `invalid type: string "1", expected u64`，记录被跳过，index 0 丢失。

## 2. 修复

- `DiskLogStore` 新格式：`u32 BE length + bincode(DiskRecord)`；
- 旧 JSONL 自动识别并迁移；
- 旧 membership 记录手工重建：configs -> `Membership::new_with_defaults`；
- 下一次 truncate/purge 重写为 bincode；
- 新增 `legacy_membership_record_parses` 单测。

## 3. 4 节点验证

```text
4 broker（quorum 3）
初始 BATCH 5/5、TX commit=1 rollback=0
kill follower B 后 BATCH 5/5
kill leader D + B 仍停 -> 2/4 无 quorum，发送挂起（符合预期）
重启 B（旧 JSONL）-> 成功加载并监听
```

重启后 B 未能恢复与 leader 的复制（仍在调查），因此 4 节点故障恢复未完全通过。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（87 项）
```

## 5. 边界

- 4 节点重加入复制未完成；
- 旧 JSONL 迁移只覆盖 membership，其他损坏记录会跳过；
- 新格式与旧二进制不兼容。
