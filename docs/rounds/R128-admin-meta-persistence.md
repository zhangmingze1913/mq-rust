# R128 · 空 Topic / 订阅组元数据持久化

> 日期：2026-11-25  
> 结果：空 topic 与订阅组写入 `admin_meta.json`，重启后恢复；Rust 测试 80 项。

---

## 1. 实现

- `admin_meta.json`：

```json
{"topics":["EMPTY_TOPIC"],"groups":["empty_group"]}
```

- `create_topic` / `delete_topic` / `create_subscription_group` / `delete_subscription_group` 后持久化；
- `open` replay 后加载，空 topic 重建 8 队列，组写入 extra_groups。

## 2. 验证

新增单测：

```text
admin_meta_survives_reopen ... ok
```

```text
create EMPTY_TOPIC + empty_group
drop / reopen
topic_names 包含 EMPTY_TOPIC
subscription_groups 包含 empty_group
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       80 项通过
```

## 4. 限制

- 只保存名称，不保存队列数/组配置；
- 删除 topic 不清理 CommitLog；
- admin_meta 非原子写；
- 多副本未同步。
