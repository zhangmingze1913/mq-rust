# R178 · Raft leader 自动发布 namesrv 路由

> 日期：2026-11-26  
> 结果：3 broker Raft 集群 leader 变更后自动更新 namesrv 路由；kill leader 后 SEND/Pull 仍通过。

---

## 1. 实现

Broker 新增：

```text
--advertise 127.0.0.1:10921
--namesrv-admin 127.0.0.1:9878
```

`spawn_raft_route` 每 500ms 检查 `raft.is_leader()`，从非主变主时：

```text
POST /broker/<advertise>
```

namesrv 更新 `broker_addr` 并 `epoch++`。

## 2. 首轮

```text
namesrv: {"broker_addr":"127.0.0.1:10941","epoch":2}
broker-c: published raft leader route
mqadmin SEND_OK
A/B/C stats: messages=1
```

## 3. leader 故障

kill broker-c：

```text
namesrv: {"broker_addr":"127.0.0.1:10931","epoch":3}
mqadmin SEND_OK
PULL_ONLY received=1
```

路由自动切到新 leader broker-b，无需手工 POST。

## 4. 边界

- 路由发布窗口 < 1s；
- 未接 admin token；
- 被 kill 的 broker 未重启/追平；
- MemLogStore 重启丢日志；
- BATCH/事务/延时未走 Raft。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
