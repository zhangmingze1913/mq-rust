# R217 · 重启后去重测试（4 节点重复仍存在）

> 日期：2026-11-27  
> 结果：BrokerStore 关闭/重开后重复 msg_id 仍只保留 1 条；但 4 节点重启复现 B 多出 5 条，未能归因。

---

## 1. 新增测试

```text
duplicate_msg_id_is_idempotent_after_reopen
```

流程：

```text
open store -> append msg -> drop
reopen -> append same msg
read -> 1 条
```

结果通过。

## 2. 4 节点复现

```text
B 重启前：A/B/C 各 5
B 重启后：A/C 15，B 20（MQ_R26A 10）
```

B 重放 Raft 日志后多出 5 条 A 消息。

## 3. 分析

- BrokerStore 去重对“关闭/重开 + 单条 append”有效；
- 4 节点重复可能来自 B 重启前未持久化该批消息，或 Raft 状态机重放与 BrokerStore 去重时序；
- 日志未保留 B 重启前 stats，无法确认；
- 需增加 apply 幂等/水位持久化（snapshot last_applied 或 apply index 记录）。

## 4. 结论

- 去重基础能力通过；
- 4 节点重启重复仍开放；
- 需在 MessageSM 持久化 applied index，避免重启重放旧日志。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（88 项）
```
