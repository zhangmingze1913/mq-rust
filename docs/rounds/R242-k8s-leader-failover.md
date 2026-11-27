# R242 · K8s 4 broker leader 故障切换

> 日期：2026-11-27  
> 结果：kill leader broker-3 后 broker-2 当选；namesrv 路由 epoch 2→3；重启 broker-3 追平；四副本均 30 条一致。

---

## 1. 步骤

```text
4 broker Raft，namesrv route leader=broker-3（epoch 2）
kubectl delete pod mq-rust-broker-3
30s 后 broker-3 重启 1/1
route leader=broker-2（epoch 3）
in-cluster MqRustCompatSmoke -> received=15
```

## 2. 结果

```text
broker-0 messages=30
broker-1 messages=30
broker-2 messages=30
broker-3 messages=30

topics: MQ_K8S4 15 + MQ_FAIL 15
```

原 15 条未丢，切换后新增 15 条四副本一致。

## 3. 说明

- 静态 namesrv 路由由 leader 发布，epoch 递增；
- 重启节点通过 PVC + 日志追平；
- 写转发保证客户端连到 follower 仍可写。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
K8s 4 broker kill leader 15/15，四副本一致
```
