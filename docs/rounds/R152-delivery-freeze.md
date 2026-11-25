# R152 · 交付冻结审计

> 日期：2026-11-25  
> 结论：交付物冻结为 v0.9.0-rc.1；功能/容器/管理面完成，**性能与严格 HA/安全仍未达 1.0**。

---

## 1. 版本

```text
HEAD   907505e perf: batch256 result; refresh dist
tag    v0.9.0-rc.1
dist   dist/mq-rust-0.9.0-rc.1-win64.zip
SHA256 AE8794F23DE59C87091CDA4FB5CAAB4B5D47F16EF00B23E0B9A533085F2943F7
docs   check-docs OK
```

## 2. 功能

- 普通/顺序/事务/定时/批量/死信/重试/Inbox；
- SQL92 Push、StartDeliver、消费组 CRUD；
- mqadmin 主要命令；
- 轨迹 JSONL/轮转/采样/查询；
- ACL topic + group、IP/CIDR；
- quorum + auto-promote + fence。

## 3. 容器

```text
Docker Compose 5 容器 + TC leader=2
K8s 5 Pod + TC leader=2 + broker/TC PVC
Pod 重建 15/15
容器内 14 类 SDK 矩阵
完整 Compose：MQ-rust + RocketMQ + Prometheus
```

## 4. 性能

```text
单条 1M          8,438 TPS
batch64          24,982 msg/s
batch128         30,860 msg/s
batch256         37,095 峰值
消费 20 线程     11,130 TPS
目标             50,000
```

## 5. 质量

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
git status                   干净
```

## 6. 未达 1.0

1. 5 万 TPS：硬件（HDD/4 核）限制，batch256 仍 37k；
2. 严格 Raft 选举/任期：当前 quorum + fence；
3. TLS/gRPC：未实现；
4. Grafana UI 未验证；
5. kind 单节点，local-path 不跨节点。

## 7. 判定

```text
0.9.0-rc.1 可交付验收
不得声明生产 1.0
```
