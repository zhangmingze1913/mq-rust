# R145 · K8s broker PVC 持久性

> 日期：2026-11-25  
> 结果：broker 改用 2Gi PVC，删除 Pod 重建后 15 条消息仍可拉取（15/15）。

---

## 1. 清单变更

- 新增 `PersistentVolumeClaim mq-rust-broker-data`（2Gi，RWO）；
- broker Deployment `strategy: Recreate`；
- volumes 由 `emptyDir` 改为该 PVC。

## 2. 验证

```text
kubectl apply -f deploy/k8s/mq-rust.yaml
PVC mq-rust-broker-data Bound

Compat MQ_K8S_PVC -> received=15
kubectl delete pod -l app=mq-rust-broker
新 Pod Running

PullOnly MQ_K8S_PVC 15 -> received=15 expected=15
```

## 3. 集群

```text
mq-rust-broker    1/1 Running
mq-rust-namesrv   1/1 Running
mq-rust-tc-0/1/2  1/1 Running
```

## 4. 限制

- 仍为 kind 单节点；
- PVC 使用 local-path，节点丢失即丢失；
- 未做滚动升级/多副本共享卷。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
