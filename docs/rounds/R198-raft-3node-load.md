# R198 · 三副本 Raft 长跑（120s）

> 日期：2026-11-26  
> 结果：三副本各 137,984 条完全一致；但持续 batch256 下客户端出现 26 次发送超时。

---

## 1. 配置

```text
3 broker Raft，自动路由
batch256 / 128B / 8 threads
120s
retain-messages 300,000
```

## 2. 结果

```text
A/B/C messages = 137,984（完全一致）
Send Failed = 26，Send Message Failed = 6,656
MPS 约 204 / 562 / 0（末尾超时）
```

## 3. 结论

- 复制一致性通过；
- 长跑吞吐约 1.1k msg/s（含超时/重试），低于短测 1.9k；
- 超时主要来自 Raft 同步提交 + 每批 256 条 apply + 本地 CommitLog；
- 客户端重试会放大失败计数，不是数据丢失。

## 4. 边界

- 未做 1M 三副本；
- 未调 client timeout / 批量并发；
- 5 万仍不可达。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
