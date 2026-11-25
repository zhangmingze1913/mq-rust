# R175 · mq-raft 三节点消息日志原型

> 日期：2026-11-26  
> 结果：新增 `mq-raft` crate；3 节点 openraft 选举并通过 TCP 复制消息日志，三个状态机收敛。

---

## 1. 范围

- 新 crate `crates/mq-raft`；
- `MessageCommand { topic, queue_id, msg_id, body }`；
- `MessageSM`：把 committed log 追加到内存消息日志；
- openraft MemLogStore + TCP JSON RPC；
- 3 节点集群，leader 选举后 `client_write`；
- 测试断言三个节点消息日志一致。

## 2. 测试

```text
cargo test -p mq-raft
running 1 test
test tests::three_nodes_replicate_message_log ... ok
test result: ok. 1 passed
```

## 3. 未完成

- 未接入 broker SEND/Pull；
- MemLogStore 重启丢失，需持久化 RaftLogStore；
- 快照传输未启用；
- 未做网络分区/重启/落后副本追平；
- 未做批量命令与性能。

## 4. 下一步

1. 持久化 vote/term/log；
2. broker `--raft-*` 配置与 SEND 接入；
3. follower 应用状态机到 `BrokerStore`；
4. 3 broker 故障注入。
