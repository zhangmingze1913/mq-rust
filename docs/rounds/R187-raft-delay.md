# R187 · 延时消息走 Raft

> 日期：2026-11-26  
> 结果：延时 SEND 通过 `MessageCommand.deliver_at_ms` 进入 Raft；单节点延时 10s 准时投递。

---

## 1. 实现

`MessageCommand` 新增：

```text
deliver_at_ms: Option<u64>
```

- broker applier：有 `deliver_at_ms` 时调用 `store.schedule_delay`，否则 `store.append`；
- 延时 SEND 在 Raft 模式下不再本地直接 schedule，而是 `raft.write`；
- 所有副本 apply 后各自维护本地定时器。

## 2. 验证

```text
MqRustDelaySmoke
DELAY EARLY count=0
DELAY RECV delay-10s
DELAY LATE count=1
/stats messages=1
```

## 3. 语义

- 延时消息在 leader 提交后复制；
- 每个副本本地定时投递；
- 客户端只从路由 broker 拉取，不会跨 broker 重复消费；
- 副本间的定时器可能有毫秒级差异。

## 4. 边界

- 未做 3 副本延时一致性测试；
- 事务消息仍非 Raft；
- 定时器持久化仍依赖 BrokerStore 的 delay log；
- 未做批量延时/性能。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
