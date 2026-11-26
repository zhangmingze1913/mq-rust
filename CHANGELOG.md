# Changelog

## 0.9.0-rc.1 — 2026-11-25

首个 RC 交付。R1–R134 迭代完成，**不满足生产 1.0**（性能/容器/严格 HA/TLS）。

### 协议与 SDK

- RocketMQ 4.9.8 Remoting：单条、批量、拉取、心跳、位点、事务、轨迹。
- Java SDK 兼容：普通、批量、Tag、事务、顺序、定时/延时、重试、死信、幂等 Inbox、SQL92 Push。
- 双分片 CommitLog、ConsumeQueue、Index、offset 持久化。
- 消息轨迹内存 + JSONL（`--trace-path`）+ `/trace/{msgId}` + `/traces`。

### 事务

- Half/Op 队列、commit/rollback、check-back、超时回滚、重启恢复。
- 事务轨迹：tx_prepare/tx_commit/tx_rollback。
- 不承诺端到端 exactly-once，提供至少一次 + 幂等。

### 管理面

- mqadmin：clusterList、topicRoute、brokerStatus、topicStatus、topicList、updateTopic、deleteTopic、consumerProgress、consumerConnection、producerConnection。
- 空 topic/订阅组元数据 `admin_meta.json` 持久化。
- Admin `/stats`、`/trace`、`/traces`、`/audit`、`/inbox`、`/fence`、`/health`。

### HA 与运维

- 异步/同步复制、`--replica-quorum`、auto-promote、旧主 fence。
- Retention（条数/字节/TTL）、CommitLog 分段回收、CQ 对齐/重写回收。
- Prometheus `/metrics`、审计日志、Inbox、Docker/K8s 清单（静态未实测）。

### 质量

- Rust 85 项测试；Clippy `-D warnings`；Java 23 个测试源。
- 12 类 SDK/管理矩阵通过。
- 1M 单条最佳 8,438 TPS（原生 RocketMQ 15,465）。
- 批量 64 条 24,982 msg/s；消费 11,130 TPS。

### 已知限制

- 5 万 TPS 未达成。
- 容器化未实测（宿主无运行时）。
- 无严格 Raft 选举，提升为 quorum + 先到先得 + fence。
- 无 TLS/gRPC；ACL 无 group 权限。
- CommitLog 仅整段删除。
- SQL92 Pull 需先完成首次心跳。
- 轨迹查询仅内存最近 N 条；JSONL 无轮转。
