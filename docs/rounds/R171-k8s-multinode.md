# R171 · 双节点 K8s 与跨节点 TC

> 日期：2026-11-26  
> 结果：kind 双节点集群；TC 3 Pod 分布到两个节点，mqadmin 发送与 SDK Pull 通过。

---

## 1. 集群

```text
kind create cluster --name mq-rust-multi \
  --config deploy/k8s/kind-2node.yaml

mq-rust-multi-control-plane  Ready
mq-rust-multi-worker         Ready
```

`deploy/k8s/mq-rust.yaml` 为 TC 增加 podAntiAffinity（preferred）。

## 2. 调度

```text
mq-rust-broker    worker
mq-rust-namesrv   worker
mq-rust-tc-0      worker
mq-rust-tc-1      worker
mq-rust-tc-2      control-plane
```

TC 不再全部堆在单节点。

## 3. 验证

```text
mqadmin sendMessage -t MQ_K8S_MULTI
SEND_OK

MqRustPullOnly
PULL_ONLY received=1 expected=1

TC leader node=3（control-plane）
```

删除 leader Pod `mq-rust-tc-2` 后，StatefulSet 在 control-plane 重建，`mq-rust-tc-0` 日志仍显示 leader=3；集群保持可用。

## 4. 限制

- 完整默认 SDK smoke 在 port-forward + hosts 方式下挂起（producer 能注册，SEND 未完成）；
- mqadmin 发送与 Pull-only 通过；
- 只测 2 节点，未做网络分区；
- kind 控制面去掉了 NoSchedule taint 以便调度。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
