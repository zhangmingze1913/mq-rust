# R85 · 分段/保留/鉴权后全量回归

> 日期：2026-11-25  
> 结果：R73–R84 改动后，Compat/Batch/Tag/Tx/Order/Delay/Inbox 全部通过。

---

## 1. 结果

| 场景 | 结果 |
|---|---|
| Compat | `15/15` |
| Batch | `5/5` |
| Tag | `3/3` |
| Tx | `commit=1 rollback=0` |
| Order | `50/50 ordered=true` |
| Delay | `EARLY=0 LATE=1` |
| Inbox | `3→1` |

Broker 配置：`--cq-segment-entries 1000`、默认无损保留。

## 2. 覆盖的近期改动

- CQ 分段/段大小/跨段读；
- Retention 段删除；
- ACL Signature/Topic/IP/CIDR/热更新；
- 消息轨迹；
- CommitLog 字节指标。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       74 项通过
```

## 4. 结论

近期存储/安全/观测改动未引入 SDK 功能回归；5 万 TPS/严格 HA/容器实测仍未完成。
