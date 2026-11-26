# R177 · 三 broker Raft SEND/Pull 与 leader 切换

> 日期：2026-11-26  
> 结果：3 broker Raft 组；leader 写入后三个 BrokerStore 均 apply；kill leader 后重新选主，继续 SEND/Pull。

---

## 1. 拓扑

```text
broker-a 10921 raft 19351 id=1
broker-b 10931 raft 19352 id=2
broker-c 10941 raft 19353 id=3
namesrv  9877 / admin 9878
```

启动参数：

```text
--raft-id N
--raft-peers 1@127.0.0.1:19351,2@...:19352,3@...:19353
```

## 2. 首轮

- leader = broker-b；
- namesrv 路由指向 10931；
- mqadmin SEND_OK；
- 三个 broker `/stats` 均为 `messages=1`；
- SDK Pull-only `received=1`。

## 3. leader 故障

kill broker-b：

```text
broker-c becomes leader
```

namesrv 路由切到 10941：

```text
mqadmin SEND_OK
PULL_ONLY received=1
```

broker-a / broker-c `/stats` 均为 `messages=2`，旧数据与新数据都在。

## 4. 边界

- 路由切换仍手工 POST，未接租约自动发布；
- MemLogStore 重启丢 Raft 日志；
- kill 的 broker-b 未重启/追平；
- BATCH/延时/事务仍走非 Raft；
- 未做网络分区。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
