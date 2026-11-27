# R249 · 多节点 K8s 4 broker 通过

> 日期：2026-11-27  
> 结果：kind 4 节点（10GB/4CPU），4 broker 分居 control-plane + 3 worker；in-cluster SDK 15/15；四副本各 15 条一致。

---

## 1. 资源

```text
.wslconfig: memory=10GB, processors=4
Docker: 10.4GB / 4 CPU
kind 4 节点
broker 4 × 1Gi
control-plane 去 taint，允许承载 broker
```

## 2. 调度

```text
broker-0 -> worker
broker-1 -> worker3
broker-2 -> worker2
broker-3 -> control-plane
```

required anti-affinity 保证一节点一 broker。

## 3. 验证

```text
mq-multi4: RESULT received=15 asyncOk=5 send=true
broker-0..3 messages 全为 15
topic: MQ_MULTI4
```

## 4. 结论

- 跨 4 节点 4 broker 通过；
- 10GB/4CPU 是最低可行配置（9GB/3CPU 会因 CNI/资源失败）；
- 之前 R248 失败为资源不足，不是协议问题。

## 5. 产物

- `deploy/k8s/kind-4node.yaml`
- `deploy/k8s/raft4.yaml`（required anti-affinity + NET_ADMIN）

## 6. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
K8s 4 节点 4 broker 15/15，四副本一致
```
