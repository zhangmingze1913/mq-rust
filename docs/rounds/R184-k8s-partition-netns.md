# R184 · K8s 真正网络分区（pod netns）

> 日期：2026-11-26  
> 结果：在 TC leader Pod 的 netns 内阻断 worker 子网 Raft 端口；worker 两节点选举新 leader，旧 leader 写入失败，新 leader 写入成功。

---

## 1. 拓扑

```text
kind 双节点
tc-0  worker         10.244.1.11
tc-1  control-plane  10.244.0.8   leader=2
tc-2  worker         10.244.1.12
```

## 2. 分区注入

在 control-plane 节点找到 tc-1 容器 PID，进入其 netns：

```text
iptables -A INPUT  -s 10.244.1.0/24 -p tcp --dport 19301 -j DROP
iptables -A OUTPUT -d 10.244.1.0/24 -p tcp --dport 19301 -j DROP
```

只阻断 worker 子网，保留 kubelet 探针源地址，避免 Pod 被重启。

## 3. 结果

新 leader：

```text
tc-2 /leader: {"node":3,"leader":3,"is_leader":true}
tc-0 /leader: {"node":1,"leader":3,"is_leader":false}
```

旧 leader（隔离）：

```text
tc-1 /leader: {"node":2,"leader":2,"is_leader":true}
```

写入：

```text
tc-1 POST /tx/old/prepare -> 失败（无 quorum）
tc-2 POST /tx/new/prepare -> {"status":"prepared"}
```

## 4. 结论

- 多数派分区后正常选主；
- 旧 leader 在本地状态仍自称 leader，但 **无法提交**，不会产生已提交写冲突；
- 这是 Raft 的正常语义：leader 状态不等于提交权；
- K8s Pod netns iptables 是可行的分区注入方式。

## 5. 清理

- 删除 kind 集群；
- 停止 port-forward。

## 6. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
