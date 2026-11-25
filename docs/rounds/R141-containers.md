# R141 · 容器化实测（Docker Compose + K8s）

> 日期：2026-11-25  
> 结果：Docker 容器栈 5 容器运行，TC Raft 选出 leader；SDK 发送/拉取/事务通过。K8s 首轮 5 Pod 运行并通过 SDK，TC leader 未选出，已做清单修复但集群后续降级未复测。

---

## 1. 环境

```text
Docker Desktop 4.100.1 / Engine 29.9.3 (linux/amd64)
Docker Compose v5.7.1
kind v0.30.0（K8s v1.34.0）
```

## 2. 镜像

```text
docker build -f deploy/Dockerfile -t mq-rust:0.9.0-rc.1 .
image: mq-rust:0.9.0-rc.1 (35.2MB)
id:    be3a776b962c
```

修复：

- `.dockerignore` 排除 target/dist/.git/docs/tests；
- Rust 基础镜像 1.85 → 1.100；
- Compose `command` 与镜像 `ENTRYPOINT` 冲突：去掉命令首项二进制路径，并显式 `entrypoint`；
- Namesrv 静态路由 `--broker-addr` 改为 `127.0.0.1:10921`，便于宿主 SDK 经端口映射访问。

## 3. Docker Compose

新增 `deploy/docker-compose.lite.yml`（仅 MQ-rust，5 容器）。

```text
deploy-mq-rust-namesrv-1   Up
deploy-mq-rust-broker-1    Up
deploy-mq-rust-tc-1-1      Up
deploy-mq-rust-tc-2-1      Up
deploy-mq-rust-tc-3-1      Up
```

TC Raft：

```text
19401 {"node":1,"leader":2,"is_leader":false}
19402 {"node":2,"leader":2,"is_leader":true}
19403 {"node":3,"leader":2,"is_leader":false}
```

SDK：

```text
ACL RESULT status=SEND_OK
Compat RESULT received=15 asyncOk=5 send=true
PullOnly received=1 expected=1
Tx commit=1 rollback=0
/stats messages=17 topics=3 queues=24
```

## 4. Kubernetes（首轮）

```text
kubectl apply -f deploy/k8s/mq-rust.yaml

mq-rust-broker-...    1/1 Running
mq-rust-namesrv-...   1/1 Running
mq-rust-tc-0/1/2      1/1 Running
PVC data-mq-rust-tc-* Bound（static PV）
```

SDK（port-forward + hosts 映射）：

```text
ACL SEND_OK
Compat received=15
Tx commit=1 rollback=0
/stats messages=17
```

TC leader 首轮为 `null`。已修复：

- headless Service 增加 `publishNotReadyAddresses: true`；
- StatefulSet 节点 ID 由 0/1/2 改为 1/2/3（与已验证的 Compose 一致）；
- 新增 `deploy/k8s/kind-pv.yaml`（无动态 provisioner 时的静态 PV）。

## 5. 未完成

- 修复后的 K8s TC leader 未复测：后续 kind 集群 controller-manager/scheduler 异常、local-path helper Pod 卡住；
- 完整 Compose（含 RocketMQ 5.3.4/Prometheus/Grafana）未全部拉取完成；
- K8s 的 `mq-rust:latest` 需手工 `kind load docker-image`。

## 6. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
