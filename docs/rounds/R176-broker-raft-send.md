# R176 · broker 单节点 Raft SEND/Pull

> 日期：2026-11-26  
> 结果：broker 接入 `mq-raft`；单节点 Raft 模式下 mqadmin SEND_OK、SDK Pull-only 1/1。

---

## 1. 启动

```powershell
mq-broker.exe ... `
  --raft-listen 127.0.0.1:19352 `
  --raft-id 1 `
  --raft-peers 1@127.0.0.1:19352
```

## 2. 数据路径

```text
SEND -> 检查 is_leader
     -> raft.client_write(MessageCommand)
     -> MessageSM.apply
     -> applier: BrokerStore.append
     -> message_by_id 返回 offset
```

- follower 返回 `not raft leader`；
- Pull 读本地 `BrokerStore`；
- 非 Raft 模式保持原 quorum 路径。

## 3. 验证

```text
mqadmin sendMessage -t MQ_RAFT_DBG2
SEND_OK

MqRustPullOnly -Expected 1
PULL_ONLY received=1 expected=1

/stats messages=1
```

## 4. 限制

- 单节点验证，未跑 3 broker；
- MemLogStore 重启丢日志；
- BATCH/DELAY/事务未走 Raft；
- 未做快照/落后副本追平/分区；
- 未做性能。

## 5. 代码

- `crates/mq-raft`：3 节点日志复制 + applier；
- `crates/mq-broker`：`--raft-*`、SEND 分支、`message_by_id`；
- 84 项测试通过。
