# R218 · 持久化 applied 水位，修复 4 节点重放重复

> 日期：2026-11-27  
> 结果：MessageSM 持久化 applied LogId；4 节点重启后四副本消息数一致，无重复。

---

## 1. 变更

- `MessageSM` 新增 `applied_path`（`raft.applied`）；
- 每次 apply / install snapshot 写 applied LogId（原子替换）；
- 启动时加载 applied，作为 `applied_state` 返回；
- 避免重启后从 0 重放已应用的 Raft 日志。

## 2. 4 节点验证

```text
4 broker，quorum 3
BATCH A 5 -> kill B -> 重启 B -> BATCH C 5

A: 10
B: 10
C: 10
D: 10
```

四副本完全一致，无重复消息。

## 3. 说明

- 之前 B 重启后多出 5 条，是因为 applied 水位未持久化；
- 现在重启只重放 applied 之后的日志；
- 与 R216 linger 修复配合，4 节点恢复闭环。

## 4. 边界

- applied 文件与 snapshot 的一致性仍需 1M 长跑验证；
- 三副本/4 节点 1M 未做；
- 5 万仍受硬件限制。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（88 项）
```
