# R241 · K8s 4 broker Raft

> 日期：2026-11-27  
> 结果：kind 2 节点、broker StatefulSet 4 副本；in-cluster SDK 15/15；四个 broker 均 messages=15，副本一致。

---

## 1. 部署

```text
kind 2 节点（control-plane + worker）
broker StatefulSet 4（Raft 19301，peers 1..4）
image mq-rust:latest（R240）
```

清单：`deploy/k8s/raft4.yaml`。

## 2. 验证

```text
mq-test4: RESULT received=15 asyncOk=5 send=true
broker-0 messages=15
broker-1 messages=15
broker-2 messages=15
broker-3 messages=15
```

四副本一致。

## 3. 说明

- 4 个 broker 均调度到 worker（control-plane 未承载）；
- 单机 kind，内存 9GB；4×1.5Gi 可运行；
- namesrv 静态路由 + 写转发保证 follower 可写；
- 动态加节点 API 已在本机 4 节点验证（R228），本轮为静态 4 副本 K8s。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
K8s 4 broker 15/15，四副本一致
```
