# R188 · 三副本延时消息一致性

> 日期：2026-11-26  
> 结果：三 broker Raft 下延时 10s 消息提交到全部副本，定时到期后三副本各 1 条，消费 1 条。

---

## 1. 步骤

```text
3 broker Raft，自动路由
MqRustDelaySmoke 发送 delay-10s
等待 12s
```

## 2. 结果

```text
DELAY EARLY count=0
DELAY RECV delay-10s
DELAY LATE count=1

A/B/C /stats messages=1
```

三个副本的 delay log 各自到期投递，客户端只从路由 broker 拉取 1 条。

## 3. 说明

- 延时命令通过 Raft 复制；
- 定时器在各副本本地运行，可能有毫秒级差异；
- 客户端只路由到一个 broker，不会跨副本重复消费；
- 若副本间时钟漂移较大，投递时间会有差异。

## 4. 边界

- 未做故障切换时延时定时器迁移；
- 事务消息仍未走 Raft；
- 未做延时批量/性能。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
