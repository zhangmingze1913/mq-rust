# R182 · 三节点全集群重启

> 日期：2026-11-26  
> 结果：三 broker 同时重启后从 `raft.log` 恢复日志/投票，重新选主，旧消息保留并可继续写入。

---

## 1. 步骤

```text
3 broker 启动，发送 M1
SEND_OK，A/B/C messages=1

kill 全部 broker
重启 3 个 broker（同一 store、同一 raft.log）
```

## 2. 重启后

```text
namesrv 路由自动恢复: 127.0.0.1:10931 epoch=3
A/B/C /stats messages=1
mqadmin send M2
SEND_OK
A/B/C /stats messages=2
PULL_ONLY received=2 expected=2
```

## 3. 说明

- `DiskLogStore` 恢复 vote/term/log；
- 标记存在，不再 `initialize`；
- 重启后重新选主，自动发布路由；
- BrokerStore 持久化 + Raft 日志重放，去重后消息不重复。

## 4. 边界

- 全量 JSON 重写，日志大时性能差；
- 未做分段/快照/压缩；
- 延时/事务仍未走 Raft；
- 网络分区未测；
- 未做 1M 压测。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
