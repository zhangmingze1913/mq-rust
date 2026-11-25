# R144 · K8s TC leader 修复复测

> 日期：2026-11-25  
> 结果：kind 集群 5 Pod 全部 Ready，TC 选出 leader=2；SDK 发送/拉取/事务通过。

---

## 1. 修复

- headless Service `publishNotReadyAddresses: true`；
- StatefulSet TC ID 由 0/1/2 改为 1/2/3；
- namesrv/broker/TC 增加 readiness/liveness 探针；
- TC `terminationGracePeriodSeconds: 20`；
- kind 动态 local-path 绑定 PVC。

## 2. 集群

```text
kind create cluster --name mq-rust3
kind load docker-image mq-rust:latest --name mq-rust3
kubectl apply -f deploy/k8s/mq-rust.yaml

mq-rust-broker-...    1/1 Running
mq-rust-namesrv-...   1/1 Running
mq-rust-tc-0/1/2      1/1 Running

PVC data-mq-rust-tc-* Bound
```

## 3. TC leader

```text
TC-0: mq-tc node=1 leader=Some(2)
TC-1: mq-tc node=2 leader=Some(2)
```

## 4. SDK

```text
ACL SEND_OK msgId=7F...55520000
Compat received=15 asyncOk=5 send=true
Tx commit=1 rollback=0

/stats:
messages=17 topics=3 queues=24
commitlog_bytes=3555
```

## 5. 结论

- K8s 下 TC Raft 3 节点可选举、可服务；
- 探针使 Pod Ready 状态真实反映端口可用；
- 动态 PVC 绑定正常。

## 6. 限制

- broker 仍用 `emptyDir`，Pod 重建会丢数据；
- 未做 TC Pod 故障切换演练；
- K8s 仅 kind 单节点。

## 7. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
