# R143 · K8s 探针与优雅终止

> 日期：2026-11-25  
> 结果：K8s 清单补齐 TCP/HTTP 探针与 TC 优雅终止；修复后 TC leader 仍未能复测。

---

## 1. 变更

- namesrv：readiness/liveness `tcpSocket:9877`；
- broker：readiness 保留 `/health:9091`，新增 liveness；
- TC：readiness `tcpSocket:9191`，liveness `tcpSocket:19301`；
- TC StatefulSet `terminationGracePeriodSeconds: 20`。

YAML 校验：7 个文档，kind 顺序正确。

## 2. 复测阻断

```text
kind create cluster mq-rust3 -> Starting control-plane 卡住 >4min
kubectl context 未生成
```

Docker Desktop / WSL 下 kind 控制面反复不健康；本轮无法验证 TC leader 修复（ID 1/2/3 + publishNotReadyAddresses）。

## 3. 已证实

- Docker Compose 5 容器 / TC leader=2 / SDK / 重启持久性（R141–R142）；
- K8s 首轮 5 Pod / SDK 通过，TC leader=null（R141）。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
