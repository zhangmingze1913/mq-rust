# R174 · K8s 网络分区尝试（负结果）

> 日期：2026-11-26  
> 结果：节点级 iptables 未能切断 kind CNI 上的 Raft 流量；本轮不构成分区安全证据。

---

## 1. 环境

```text
kind 双节点：control-plane 172.19.0.3 / worker 172.19.0.4
TC：tc-0/tc-2 在 worker，tc-1 在 control-plane
leader=2（control-plane）
```

## 2. 尝试

在 worker 与 control-plane 上分别阻断：

```text
node IP 172.19.0.3/4 tcp 19301
pod IP 10.244.0.8 / 10.244.1.0/24 tcp 19301
```

## 3. 结果

```text
tc-1 /leader -> {"node":2,"leader":2,"is_leader":true}
```

leader 未降级，说明 Raft 流量仍通过 CNI/VXLAN 路径或规则未命中。

## 4. 结论

- 节点级 iptables 不适合作为 kind CNI 分区注入手段；
- 需要 NetworkPolicy/chaos-mesh 或在 veth 上做 tc；
- 本轮无分区安全结论。

## 5. 清理

kind 集群与残留容器已删除。
