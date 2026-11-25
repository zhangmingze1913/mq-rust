# R127 · 订阅组创建/删除（200/207）

> 日期：2026-11-25  
> 结果：Java Admin API 创建组后 201 可见，删除后不可见。

---

## 1. 实现

- 200 `UPDATE_AND_CREATE_SUBSCRIPTIONGROUP`：
  - 先读 ext `groupName`；
  - 再解析 body JSON `groupName`；
- 207 `DELETE_SUBSCRIPTIONGROUP`：
  - 删除 extra_groups、consumers、subscriptions、offsets；
- 201 合并 extra_groups。

## 2. 验证

新增 `MqRustGroupCrud.java`：

```text
createAndUpdateSubscriptionGroupConfig(broker, config)
getAllSubscriptionGroup -> afterCreate=true

deleteSubscriptionGroup(broker, group, false)
getAllSubscriptionGroup -> afterDelete=false
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 组配置未持久化，重启丢失；
- mqadmin updateSubGroup 在本环境仍受 clusterInfo 请求影响；
- 删除组不清理 CommitLog/CQ；
- 未实现组配置读写详情。
