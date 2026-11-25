# R30 · 最终状态与 1.0 差距评估

> 日期：2026-11-25  
> 结论：**项目未达到生产 1.0，目标未全部达成**；已交付可运行、可复现验收的单机事务消息中间件与 TC Raft 集群。

---

## 1. 完成度评估

| 目标条款 | 状态 | 证据 |
|---|---|---|
| NSQ/RocketMQ 源码与文档分析 | ✅ | `docs/01`、`02`、`14` |
| 架构/功能图谱/事务/选型/路线图 | ✅ | `docs/00`–`13`、10 张 PNG |
| `G:\MQ_soft` 基础环境 | 🟡 | JDK/Maven/RocketMQ/PlantUML；无 Docker/WSL |
| 分阶段实现 | ✅ | R1–R29，60 项 Rust 测试 |
| RocketMQ SDK 功能验收 | ✅ | 15/15、Push、Tag、Batch、Retry、DLQ、SQL92、Delay、Order |
| 事务验收 | ✅ | commit/rollback/check-back/超时/审计/人工/TC/outbox/reconcile |
| 性能验收 | 🟡 | 100k 长跑 **9,347 TPS**；目标 5 万未达成 |
| 1:1 功能/性能 | ⬜ | 缺自动 HA、ACL/TLS/gRPC、容器实测 |
| 容器化 | 🟡 | Dockerfile/compose/K8s 已备；未运行 |

---

## 2. 已交付能力

### 消息与协议
- RocketMQ 4.9.8 Remoting（JSON + 二进制 header）；
- 同步/异步/oneway、Pull/Push、长轮询、心跳；
- Tag、SQL92、批量、顺序、延时/任意投递时间、事务、DLQ、重试、Rebalance；
- mqadmin clusterList/topicList/consumerProgress。

### 存储
- CommitLog（分片/CRC/恢复、单次写）；
- ConsumeQueue 持久化 + 单/多分片冷读；
- msgId 索引、消费位点持久化、启动/周期压缩；
- Half/retry/delay WAL、审计 JSONL。

### 事务
- TC：openraft 单节点/三节点 TCP、kill leader、follower 读、WAL、Raft log、快照、压缩；
- Broker↔TC 多地址、持久化 outbox、按 TC 决策收口 Half；
- 消费 Inbox（显式 API + SDK demo）。

### HA
- Broker 主从复制（异步 writer、断连重放、累计 ack）；
- Namesrv 路由更新 + Slave 自动切换（最小防脑裂）。

### 部署与运维
- 原生单机/三 TC 脚本；
- Dockerfile/compose、K8s StatefulSet；
- `/metrics`、admin `/health`、`/tx`、`/inbox`、`/replica`。

---

## 3. 性能

| 场景 | TPS |
|---|---|
| 短跑 20k / 8 线程 | 6.0–6.6k |
| 100k 长跑第 1 次 | 7,717 |
| 100k 长跑第 2 次 | **9,347** |
| 100k 长跑第 3 次 | 9,032 |

环境：i5-4210M / HDD / 1 KiB / 8 线程 / 单机。

---

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
RocketMQ SDK 回归             通过
```

---

## 5. 明确未完成

1. 5 万 TPS（当前 9,347）。
2. Docker/K8s 实测（宿主无运行时）。
3. 自动 HA 的共识/严格防脑裂；复制逐条校验。
4. ACL/TLS/gRPC/消息轨迹。
5. CQ 轮转/清理、运行期长期稳定性。
6. Inbox 自动拦截/分布式去重表。

---

## 6. 后续路线

| 优先级 | 工作 |
|---|---|
| P0 | 多连接 pipeline、批量提交、NVMe 复测，冲 1 万+ |
| P0 | TC 参与 broker 选主/租约，严格防脑裂 |
| P1 | Docker/K8s 环境实测与 CI |
| P1 | CQ 轮转、长期稳定性/混沌 |
| P2 | ACL/TLS/gRPC/轨迹、分布式 Inbox |

---

## 7. 结论

MQ-rust 已具备**可运行、可复现验收**的事务消息中间件主干，但性能、容器实测、严格 HA 与安全仍未达到生产 1.0。  
**目标未全部达成，goal 保持 active**；后续需继续 P0/P1 工作。
