# R200 · GitHub 同步核对与 batch64 调优（负结果）

> 日期：2026-11-26  
> 结果：GitHub main/tag 与本地一致；batch64 吞吐更低且出现 follower 滞后，未采用。

---

## 1. GitHub 同步

```text
local main:  a2e8e4b
remote main: a2e8e4b
local tag:   ffe3284 (-> a2e8e4b)
remote tag:  ffe3284 (-> a2e8e4b)
```

已确认同步。

## 2. batch64 试验

```text
3 副本 Raft，batch64，60s
MPS 1053 / 662 / 533
Send Failed 31 / 1984
A 57,152；B/C 62,720
```

- 吞吐低于 batch256；
- A 滞后 5,568 条；
- 停止负载 8s 后仍未追平；
- 重启 A 后恢复到 62,720，说明日志未丢，是 apply/追平滞后。

## 3. 结论

- batch64 不采用；
- batch256 仍是较优配置；
- follower 追平滞后需要后续优化（批量 apply/写入合并）；
- 不是数据丢失，重启可恢复。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
