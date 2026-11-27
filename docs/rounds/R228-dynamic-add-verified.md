# R228 · 动态加节点追平验证

> 日期：2026-11-27  
> 结果：release 4 节点，`/raft/add` 后 membership 变为 4 voter；写入 1 条消息四副本全部 `messages=1`。

---

## 1. 环境

```text
release 二进制
namesrv 9899
broker1..4 11001..11004
raft 19701..19704
node4 预置 .raft-initialized，以 learner 启动
```

## 2. 步骤

```text
POST node1/2/3 /raft/add?node=4&addr=127.0.0.1:19704
-> node3 {"added":4}

leader metrics:
membership voters [1,2,3,4], learners []
replication 4:T1-N3.2
```

随后 namesrv 指向 leader（11003），发送 1 条：

```text
broker-a qid=7 SEND_OK
```

## 3. 结果

```text
broker1 messages=1
broker2 messages=1
broker3 messages=1
broker4 messages=1
```

节点 4 完成日志追平并成为 voter，四副本一致。

## 4. 注意

- namesrv 静态路由必须指向 leader，follower 不转发写（返回 not raft leader）；
- 本机 3+1 节点仍有心跳 `Elapsed` 告警，但 membership/复制成功；
- K8s 镜像未包含 R227/R228 代码。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
