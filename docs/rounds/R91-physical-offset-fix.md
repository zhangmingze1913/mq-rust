# R91 · 物理偏移持久化修复 + 复制中段回收

> 日期：2026-11-25  
> 结果：修复重启后 physical_offset 丢失；master/slave 均 800 条、CommitLog 1,036,662B，复制中段回收验证通过。

---

## 1. 问题

- 写入时先 `msg.encode()`、后设置 `physical_offset`，日志中记录的是旧值；
- 重启 replay 解码得到 0，`trim_commitlogs` cutoff=0，`freed=0`；
- CQ 修复也会写入错误物理偏移。

## 2. 修复

- 直接写：加日志锁后先 `physical_offset = log.len()`，再 encode/append；
- Group Commit writer：逐条先设 offset 再 encode/append（放弃批量写，group commit 本身已为负收益）；
- replay：`persist=false` 时保留解码出的 physical_offset，不再用 `next_physical` 覆盖。

## 3. 验证

```text
全新 store，1MiB/段，retain=100/队列
5,000 发送：0 失败
MASTER /stats：800 条，commitlog=1,036,662
SLAVE  /stats：800 条，commitlog=1,036,662
commitlog-0.meta = 1,048,338
PULL_ONLY 800/800，offset 525..625
slave 重启：800 条，commitlog=1,036,662
```

单测：

```text
reopen_preserves_physical_offsets ... ok
retention_trims_commitlog_and_persists_base ... ok
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       78 项通过
```

## 5. 限制

- Group Commit 已退化为逐条写；
- 仅 1 主 1 从；
- 旧版本数据中的 physical_offset 仍为旧值。
