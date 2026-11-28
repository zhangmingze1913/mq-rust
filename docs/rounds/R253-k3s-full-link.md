# R253 · K8s（Kiri k3s）全链路压测

> 日期：2026-11-28  
> 结果：Kiri k3s 单节点 3 broker，SDK 15/15；全链路 60s 27,648 发送、2,304 失败、p99 16.2s，明显低于 Kiri Docker。

---

## 1. 环境

```text
WSL kiri + kiri k8s（k3s v1.37.1，单节点 kiri）
mq-rust 镜像导入 k3s containerd
3 broker StatefulSet + namesrv
mq-rust-test 镜像导入
node: kiri（4 CPU 共享）
```

## 2. 功能

```text
k3s-smoke2: RESULT received=15 asyncOk=5 send=true
```

## 3. 全链路 60s

```text
MqRustFullLink K3S_FL 2 4 60 256 3000
sent=27,648
failed=2,304
received=29,440
p50=582ms
p99=16,244ms
```

## 4. 对比 Kiri Docker

```text
Kiri Docker 2P/4C: 489,216/300s，p99 4.45s
Kiri k3s 3 broker 单节点: 27,648/60s，p99 16.2s
```

K3s 单节点 + CNI + 控制面开销显著，性能远低于 Kiri Docker。

## 5. 说明

- 前期失败是多个挂起 Java pod 占满 CPU；清理后 SDK 通过；
- k3s 单节点不代表多节点 K8s 性能；
- 全链路功能可用，性能需更大节点/多机。

## 6. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
K3s SDK 15/15
```
