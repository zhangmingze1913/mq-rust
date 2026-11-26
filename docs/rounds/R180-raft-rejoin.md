# R180 · Raft follower 重启追平

> 日期：2026-11-26  
> 结果：通过 `.raft-initialized` 标记避免重启时重新 initialize；kill 的 follower 重启后从 leader 追平缺失日志。

---

## 1. 问题

R177 的 kill/重启会再次 `raft.initialize()`，导致重启节点自选主、与现集群分裂。

## 2. 修复

- broker 在 `store_root/.raft-initialized` 写标记；
- 首次启动：`initialize=true`，写标记；
- 重启：`initialize=false`，以 Learner 加入现集群；
- leader 从 nextIndex 回退，补齐 follower 缺失日志；
- BrokerStore 幂等去重避免重复 apply。

## 3. 验证

```text
M1 -> A/B/C messages=1
kill A
M2 -> B/C messages=2
restart A（不 initialize，带标记）
A /stats messages=2
C /stats messages=2
```

A 启动日志显示空 Raft state、`is_leader=false`，随后从 leader 追平。

## 4. 边界

- **MemLogStore 仍不持久化**：leader 或全集群重启会丢 Raft 日志；
- 标记只避免重复 initialize；
- follower 追平依赖 leader 尚存日志；
- 快照/落后压缩未做。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
