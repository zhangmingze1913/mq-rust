# R243 · K8s 网络延迟注入

> 日期：2026-11-27  
> 结果：broker-0 注入 100ms netem 延迟后，in-cluster SDK 15/15；四副本均 45 条一致；移除后恢复。

---

## 1. 步骤

```text
给 broker StatefulSet 加 NET_ADMIN
broker-0 安装 iproute2
tc qdisc replace dev eth0 root netem delay 100ms
mq-net: MqRustCompatSmoke -> received=15
tc qdisc del dev eth0 root
```

## 2. 结果

```text
RESULT received=15 asyncOk=5 send=true
broker-0..3 messages 全为 45
topics: MQ_K8S4/MQ_FAIL/MQ_NET 各 15
```

Raft 复制在单节点 100ms 延迟下仍完成三副本提交，无消息丢失。

## 3. 变更

- `deploy/k8s/raft4.yaml` 增加 `securityContext.capabilities.add: [NET_ADMIN]`，用于故障注入。

## 4. 边界

- 仅 100ms 延迟，未做丢包/分区；
- tc 需临时安装 iproute2；
- 延迟期间健康探针未失败。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
K8s 延迟注入 15/15，四副本一致
```
