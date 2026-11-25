# R126 · 订阅组列表 201

> 日期：2026-11-25  
> 结果：`getAllSubscriptionGroup` 返回在线消费组 1 个。

---

## 1. 实现

- 请求码 201 `GET_ALL_SUBSCRIPTIONGROUP_CONFIG`；
- 从 consumers 与 subscriptions 汇总 group；
- 返回：

```json
{
  "subscriptionGroupTable": {
    "mq_rust_offset_progress_group": {
      "groupName": "...",
      "consumeEnable": true,
      "retryMaxTimes": 16,
      "retryQueueNums": 1,
      ...
    }
  }
}
```

## 2. 验证

新增 `MqRustGroupList.java` 调用 `DefaultMQAdminExt.getAllSubscriptionGroup`：

```text
GROUP_LIST count=1
GROUP mq_rust_offset_progress_group
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 仅 broker 内存中的组；
- 未持久化空组；
- 4.9.8 mqadmin 无 subscriptionGroupList 子命令；
- 未实现 create/delete 组。
