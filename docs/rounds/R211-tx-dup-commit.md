# R211 · 重复 END_TRANSACTION 幂等

> 日期：2026-11-26  
> 结果：同一事务提交两次 END_TRANSACTION，消息仍只出现 1 条。

---

## 1. 测试

新增 `MqRustTxDupCommit`：

- 事务消息本地状态 COMMIT；
- 取 txId/msgId/broker 地址；
- 直接调用 `MQClientAPIImpl.endTransactionOneway` 两次；
- 拉取计数。

## 2. 结果

```text
DUP RESULT tx=7F...0000 count=1
/stats messages=1
```

## 3. 语义

- 第一次 commit 移除 half 并 append；
- 第二次 commit 找不到 half，Raft applier 返回 None；
- 不产生重复消息；
- 客户端重试提交安全。

## 4. 边界

- 只验证重复 commit，未验证 leader 在 commit 返回前崩溃的客户端重试；
- 未验证 rollback 重复；
- 未验证跨 leader 重试。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（87 项）
Java DUP 测试通过
```
