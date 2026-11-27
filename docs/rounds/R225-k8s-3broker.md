# R225 · K8s 三 broker Raft 实测

> 日期：2026-11-27  
> 环境：H:\mq，kind 3 节点，mq-rust:latest（152MB），in-cluster Java SDK  
> 结果：3 broker Raft 在 K8s 正常选主/收发；in-cluster SDK 15/15；70s 约 114k 条，26 次超时。

---

## 1. 部署

```text
kind: mq-rust-k8s（control-plane + 2 worker）
namespace: mq-rust
namesrv: 1
broker: StatefulSet 3（Raft 19301）
image: mq-rust:latest
test image: mq-rust-test:latest
```

清单：`deploy/k8s/raft3.yaml`、`deploy/k8s/kind-3node.yaml`。

## 2. 功能

```text
mq-rust-namesrv route: mq-rust-broker-2...:10921
in-cluster MqRustCompatSmoke: RESULT received=15 asyncOk=5 send=true
```

## 3. 性能

```text
batch256 / 256B / 4 threads / 70s
messages: 114,432（K8S_PERF）
Send Failed: 26
MPS 1365–2816
Max RT 2883ms
broker CPU limit 1 core，宿主 4 核
```

## 4. 结论

- K8s 3 broker Raft 可用；
- 容器内 SDK 通过；
- 单 broker 1 CPU limit 下约 1.6k msg/s；
- 5 万仍不可达，需更大宿主/多 broker 分流。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
K8s in-cluster SDK 15/15
```
