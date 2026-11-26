# MQ-rust

> Rust 实现的分布式事务消息中间件，兼容 Apache RocketMQ 4.9.x Remoting 协议与 Java SDK。
> 面向电商/金融等对“低延迟、高可靠、可审计、最终一致”有要求的场景。

- **当前阶段**：R1–R194 完成；RC 已验收；事务提交改为 Raft-first
- **测试**：85 项 Rust 测试通过；RocketMQ 4.9.8 Java SDK 全量回归通过
- **性能**：单条 100k 长跑 **9,347 TPS**；1M 长跑 **8,438 TPS/0 失败**；批量 64 条 **24,982 msg/s**；消费 20 线程 **11,130 TPS**
- **目标**：兼容 RocketMQ 协议/SDK 的单机事务消息中间件 + TC Raft 集群

---

## 1. 架构

```text
Producer/Consumer (RocketMQ Java SDK)
        │ Remoting 4.9.8
        ▼
   mq-namesrv  ◄──── 路由/集群信息
        │
        ▼
   mq-broker
   ├─ CommitLog（分片、CRC、恢复）
   ├─ ConsumeQueue（持久化索引、冷读）
   ├─ msgId/key 索引
   ├─ Half / retry / delay WAL
   ├─ consumer offset 持久化
   └─ TC outbox / reconciliation
        │ HTTP
        ▼
   mq-tc ×3（openraft TCP Raft + WAL + snapshot + 压缩）
```

设计文档见 [00-项目总览](docs/00-项目总览.md)、[06-技术架构](docs/06-技术架构.md)、[04-关键关注点](docs/04-关键关注点.md)。

---

## 2. 已实现功能

| 维度 | 状态 |
|---|---|
| 普通消息 / 同步 / 异步 / oneway | ✅ Java SDK 15/15 |
| Push / Pull / 长轮询 / 心跳 | ✅ |
| Tag / SQL92 | ✅ |
| 顺序消息 | ✅ 50 条有序 |
| 批量消息 | ✅ |
| 延时消息 / 任意投递时间 | ✅ 18 级 + `__STARTDELIVERTIME` |
| 事务消息 | ✅ commit/rollback/check-back/超时 |
| 死信 DLQ | ✅ attempts=2、dlq=1 |
| 重试 / Rebalance | ✅ |
| 幂等生产 | ✅ `(topic,msgId)` |
| 人工事务干预 | ✅ `POST /tx/{id}/commit|rollback` |
| 消费位点持久化 | ✅ `offsets` 日志 + 启动/周期压缩 |
| 消费 Inbox | ✅ `POST /inbox/{group}/{msg_id}` |
| 存储观测 | ✅ R40（`/stats` + `/metrics` gauges） |
| CommitLog 字节 | ✅ R84（`commitlog_bytes`） |
| 消息保留上限 | ✅ R42（条数/字节/TTL） |
| Retention 段删除 | ✅ R76（对齐段直接删除，否则重写） |
| Retention 指标 | ✅ R56（`retained_dropped_total`） |
| 按 Topic 统计 | ✅ R57（`/stats` `topics_detail`） |
| ConsumeQueue 持久化 + 冷读 | ✅ 单/多分片 + 可配置分段 |
| CommitLog 分段 | ✅ R86（`--commitlog-segment-bytes`） |
| CommitLog 段回收 | ✅ R88（Retention 后删整段） |
| 物理偏移持久化 | ✅ R91（重启/复制/trim 一致） |
| msgId 索引 | ✅ `index` 日志 |
| TC 三节点 TCP Raft | ✅ 多进程、kill leader、follower 读 |
| TC 状态 WAL / Raft log / snapshot / 压缩 | ✅ |
| Broker↔TC 多地址 + outbox | ✅ TC 宕机/重启补齐 |
| Broker 按 TC 决策收口 Half | ✅ SDK E2E |
| 原生等价部署 | ✅ `deploy/native` |
| Docker/K8s 资产 | 🟡 `deploy/Dockerfile`、compose、`k8s/mq-rust.yaml` 已备，未实测 |
| 5 万 TPS | 🟡 100k 长跑 9,347；短跑 6.0–6.6k |
| ACL/TLS/gRPC | 🟡 AccessKey + Signature + Topic 权限 + IP/CIDR/热更新；TLS/gRPC/group 权限未完成 |
| 消息轨迹 | ✅ R78/R79（send/pull + 事务/死信） |
| Broker 主从复制 + 重放 + 自动切换 + ack | ✅ `--promote-to`；`GET /replica` 返回累计 ack |
| 旧主路由栅栏 | ✅ R44（`--fence-url`，路由不符只读） |
| 同步复制 ACK | ✅ R58（`--sync-replicate`，等待副本确认） |
| 路由 epoch 栅栏 | ✅ R45（epoch 持久化，旧 epoch 仍只读） |
| 事件驱动栅栏 | ✅ R46（Namesrv `--fence-notify` → Broker `/fence`） |
| 栅栏鉴权 | ✅ R47（`--fence-token`，Bearer） |
| Admin 鉴权 | ✅ R48（Namesrv `--admin-token`） |
| Broker Admin 鉴权 | ✅ R49（`--admin-token`，401/200） |
| Namesrv 路由持久化 | ✅ R52（`--route-file`） |

---

## 3. 文档地图

| 文档 | 内容 |
|---|---|
| [00-项目总览](docs/00-项目总览.md) | 目标、边界、里程碑 |
| [01-NSQ架构分析](docs/01-NSQ架构分析.md) | NSQ 架构与取舍 |
| [02-RocketMQ架构分析](docs/02-RocketMQ架构分析.md) | CommitLog/CQ/HA/事务 |
| [03-功能图谱](docs/03-功能图谱.md) | RocketMQ 功能对照 |
| [04-关键关注点](docs/04-关键关注点.md) | 一致性、可用性、性能、安全 |
| [05-事务性设计](docs/05-事务性设计.md) | Half/Op/check-back/幂等 |
| [06-技术架构](docs/06-技术架构.md) | Rust 分层与数据流 |
| [07-Rust技术选型](docs/07-Rust技术选型.md) | Tokio/openraft/bytes 等 |
| [08-实现方案](docs/08-实现方案.md) | 阶段划分 |
| [09-路线图与迭代计划](docs/09-路线图与迭代计划.md) | 当前进度 |
| [10-测试与验收](docs/10-测试与验收.md) | 兼容/功能/性能/混沌 |
| [11-性能基准与容量规划](docs/11-性能基准与容量规划.md) | 指标与容量公式 |
| [12-可观测性与监控图谱](docs/12-可观测性与监控图谱.md) | `/metrics`、Prometheus |
| [13-第一轮执行记录](docs/13-第一轮执行记录.md) | 首轮协议核对与实现 |
| [14-源码核对记录](docs/14-源码核对记录.md) | 协议字段核对 |
| [15-交付清单](docs/15-交付清单.md) | 交付物与验收矩阵 |
| [R30-最终状态](docs/rounds/R30-final.md) | 完成度评估与 1.0 差距 |
| [R31-最终回归](docs/rounds/R31-final-regression.md) | R30 后全链路回归 |
| [R32-原生栈复验](docs/rounds/R32-native-final.md) | 5 端口 + SDK/事务/Inbox |
| [R33-一致性检查](docs/rounds/R33-consistency.md) | 质量/文档/YAML/二进制 |
| [R34-80轮封版](docs/rounds/R34-final-80.md) | 最终完成度与 backlog |
| [R35-性能实验](docs/rounds/R35-perf-experiments.md) | 多连接/索引写优化（负结果） |
| [R36-批量性能](docs/rounds/R36-batch-performance.md) | 16/64 条批量测速 |
| [R37-并发一致性](docs/rounds/R37-concurrency.md) | 8 线程 ×100 条压力测试 |
| [R38-CQ修复](docs/rounds/R38-cq-repair.md) | 启动校验/重写 CQ |
| [R39-队列调优](docs/rounds/R39-queue-tuning.md) | 4/8/16 队列对比 |
| [R40-存储统计](docs/rounds/R40-admin-stats.md) | `/stats` API |
| [R41-崩溃恢复](docs/rounds/R41-crash-recovery.md) | kill 后 15/15 恢复 |
| [R42-Retention](docs/rounds/R42-retention.md) | 每队列保留上限 |
| [R44-路由栅栏](docs/rounds/R44-router-fencing.md) | 旧主只读 |
| [R45-Epoch栅栏](docs/rounds/R45-epoch-fencing.md) | 旧 epoch 仍只读 |
| [R46-事件栅栏](docs/rounds/R46-event-fencing.md) | 路由变更即时通知 |
| [R47-栅栏鉴权](docs/rounds/R47-fence-token.md) | Bearer token + body 修复 |
| [R48-Admin鉴权](docs/rounds/R48-admin-auth.md) | Namesrv 401/200 |
| [R49-Broker鉴权](docs/rounds/R49-broker-admin-auth.md) | `/stats` 401/200 |
| [R50-100轮里程碑](docs/rounds/R50-milestone-100.md) | 最终回归与完成度 |
| [R51-批量扫描](docs/rounds/R51-batch-sweep.md) | 16/64/1024 条对比 |
| [R52-路由持久化](docs/rounds/R52-route-persistence.md) | 重启保留地址/epoch |
| [R54-原生复验](docs/rounds/R54-native-revalidation.md) | 5 端口 + 15/15 |
| [R55-ACL原型](docs/rounds/R55-acl-prototype.md) | AccessKey 白名单 |
| [R56-Retention指标](docs/rounds/R56-retention-metric.md) | 丢弃计数 |
| [R57-Topic统计](docs/rounds/R57-topic-stats.md) | `/stats` 明细 |
| [R58-同步复制](docs/rounds/R58-sync-replication.md) | ack 等待/超时 |
| [R59-全量回归](docs/rounds/R59-full-regression.md) | Tag 首次 flake |
| [R60-Tag稳定性](docs/rounds/R60-tag-stability.md) | 5 次 3/3 通过 |
| [R61-版本接口](docs/rounds/R61-version-endpoint.md) | `/version` |
| [R62-缺口刷新](docs/rounds/R62-readme-gaps.md) | README 对齐 |
| [R63-交付终检](docs/rounds/R63-delivery-consistency.md) | 构建/文档/YAML |
| [R64-验收矩阵](docs/rounds/R64-acceptance-matrix.md) | 证据项刷新 |
| [R65-环境终审](docs/rounds/R65-environment-audit.md) | 无容器运行时 |
| [R66-最终差距](docs/rounds/R66-final-gap.md) | 116 轮完成度 |
| [R67-Git快照](docs/rounds/R67-git-snapshot.md) | 本地版本库 |
| [R68-最终收口](docs/rounds/R68-final-closure.md) | 118 轮结论 |
| [R69-ACL签名](docs/rounds/R69-acl-signature.md) | HMAC 校验 |
| [R70-ACL权限](docs/rounds/R70-acl-policy.md) | Topic 读/写 |
| [R71-ACL热更新](docs/rounds/R71-acl-hot-reload.md) | 周期重载 |
| [R72-ACL IP](docs/rounds/R72-acl-ip.md) | 来源 IP 白名单 |
| [R73-CQ分段](docs/rounds/R73-cq-segments.md) | 100k 条/段 |
| [R74-分段性能](docs/rounds/R74-segmented-cq-perf.md) | 9,164 TPS 无回归 |
| [R75-段大小配置](docs/rounds/R75-cq-segment-config.md) | 跨段 60/60 |
| [R76-Retention段删](docs/rounds/R76-retention-segment-drop.md) | 对齐删除 |
| [R77-ACL CIDR](docs/rounds/R77-acl-cidr.md) | IPv4/IPv6 CIDR |
| [R78-消息轨迹](docs/rounds/R78-message-trace.md) | send/pull 轨迹 |
| [R79-事务轨迹](docs/rounds/R79-tx-trace.md) | prepare/commit/rollback |
| [R80-消费吞吐](docs/rounds/R80-consumer-throughput.md) | 11,130 TPS |
| [R81-GroupCommit](docs/rounds/R81-group-commit.md) | 4,155（负结果） |
| [R82-CQ单次写](docs/rounds/R82-cq-single-write.md) | 3 写合 1 |
| [R83-Retention恢复](docs/rounds/R83-retention-crash.md) | kill 后 400/400 |
| [R84-CommitLog字节](docs/rounds/R84-commitlog-metric.md) | 3120B/15 条 |
| [R85-全量回归](docs/rounds/R85-regression.md) | 7 类场景通过 |
| [R86-CommitLog分段](docs/rounds/R86-commitlog-segments.md) | 15 段/重启 15/15 |
| [R87-200k压力](docs/rounds/R87-stress-retention.md) | 丢弃 196k/保留 4k |
| [R88-CommitLog回收](docs/rounds/R88-commitlog-trim.md) | 84MB→16.3MB |
| [R89-回收后写入](docs/rounds/R89-post-trim-append.md) | offset 接续 |
| [R90-双副本保留](docs/rounds/R90-replication-retention.md) | 均 4,000/4,000 |
| [R91-物理偏移修复](docs/rounds/R91-physical-offset-fix.md) | 复制中 trim 通过 |
| [R92-1M压力](docs/rounds/R92-million-stress.md) | 4,572 TPS/16 失败 |
| [R93-CQ非对齐重写](docs/rounds/R93-cq-unaligned-rewrite.md) | 0 次 repair |
| [R94-1M复测](docs/rounds/R94-million-retest.md) | 8,438 TPS/0 失败 |
| [R95-Quorum复制](docs/rounds/R95-quorum-replication.md) | 三副本一致 |
| [R96-自动提升](docs/rounds/R96-auto-failover.md) | 提升后 110 条 |
| [R97-旧主Fence](docs/rounds/R97-fence-old-master.md) | read_only=true |
| [R98-双分片回归](docs/rounds/R98-final-functional.md) | 7 类场景通过 |
| [R99-最终报告](docs/rounds/R99-final-report.md) | 功能 RC，未达 1.0 |
| [R100-SDK边界](docs/rounds/R100-sdk-edge-modes.md) | 8 类通过；SQL92 Pull 时序 |
| [R101-定时重启](docs/rounds/R101-startdeliver-restart.md) | 恢复后 1/1 |
| [R102-半事务重启](docs/rounds/R102-tx-half-restart.md) | 提交后 visible=1 |
| [R103-超时回滚](docs/rounds/R103-tx-timeout-restart.md) | pending 0 |
| [R104-交付冻结](docs/rounds/R104-delivery-freeze.md) | v0.9.0-rc.1 |
| [R105-分发包](docs/rounds/R105-dist-package.md) | 2.83MB zip |
| [R106-分发冒烟](docs/rounds/R106-dist-smoke.md) | 包内二进制通过 |
| [R107-运维手册](docs/rounds/R107-runbook.md) | 端口/HA/排障 |
| [R108-容器差距](docs/rounds/R108-container-gap.md) | 无运行时；YAML 通过 |
| [R109-1.0差距](docs/rounds/R109-gap-review.md) | 10 项量化 |
| [R110-一致性复核](docs/rounds/R110-final-consistency.md) | 包含全部文档 |
| [R111-交付索引](docs/rounds/R111-delivery-index.md) | 交付物索引 |
| [R112-迭代收口](docs/rounds/R112-iteration-close.md) | 160/160 结论 |
| [R113-原生基线](docs/rounds/R113-native-baseline.md) | 同机 15,465 TPS |
| [R114-无保留1M](docs/rounds/R114-no-retention-1m.md) | 4,073/32 失败 |
| [R115-跳过复制编码](docs/rounds/R115-replica-skip.md) | 4,481/0 失败 |
| [R116-Retention调优](docs/rounds/R116-retention-tuning.md) | 500/1s 最佳 |
| [R117-容器清单修复](docs/rounds/R117-compose-k8s-fix.md) | 端口/peer ID |
| [R118-mqadmin兼容](docs/rounds/R118-mqadmin-202-203.md) | topicStatus 可用 |
| [R119-消费管理](docs/rounds/R119-consumer-admin.md) | progress/connection |
| [R120-连接字段](docs/rounds/R120-consumer-connection-fields.md) | 订阅/类型完整 |
| [R121-真实地址](docs/rounds/R121-consumer-address.md) | clientAddr 真实 |
| [R122-CQ段调优](docs/rounds/R122-cq-segment-tuning.md) | 125 条失败 |
| [R123-topicList](docs/rounds/R123-topic-list.md) | 真实 topic |
| [R124-updateTopic](docs/rounds/R124-update-topic.md) | 创建空 topic |
| [R125-deleteTopic](docs/rounds/R125-delete-topic.md) | broker+namesrv 删除 |
| [R126-订阅组列表](docs/rounds/R126-subscription-groups.md) | 201 返回组 |
| [R127-订阅组CRUD](docs/rounds/R127-subscription-group-crud.md) | 200/207 |
| [R128-元数据持久化](docs/rounds/R128-admin-meta-persistence.md) | 重启恢复 |
| [R129-管理后回归](docs/rounds/R129-admin-regression.md) | 7 类+admin 通过 |
| [R130-轨迹持久化](docs/rounds/R130-trace-persistence.md) | JSONL 重启可查 |
| [R131-producer连接](docs/rounds/R131-producer-connection.md) | 204 真实地址 |
| [R132-SDK矩阵](docs/rounds/R132-sdk-matrix.md) | 12 类通过 |
| [R133-traces接口](docs/rounds/R133-traces-api.md) | topic/limit 查询 |
| [R134-验收刷新](docs/rounds/R134-final-acceptance-refresh.md) | 报告至 R133 |
| [R135-交付说明](docs/rounds/R135-release-notes.md) | CHANGELOG/说明 |
| [R136-分发包验证](docs/rounds/R136-distcheck.md) | 解压即跑 |
| [R137-轨迹轮转](docs/rounds/R137-trace-rotation.md) | max-bytes |
| [R138-一致性检查](docs/rounds/R138-release-consistency.md) | check-docs.ps1 |
| [R139-轨迹异步](docs/rounds/R139-trace-async.md) | 采样/后台线程 |
| [R141-容器实测](docs/rounds/R141-containers.md) | Compose/K8s |
| [R142-容器重启](docs/rounds/R142-container-restart.md) | 消息不丢 |
| [R143-K8s探针](docs/rounds/R143-k8s-probes.md) | 探针/终止 |
| [R144-K8s TC](docs/rounds/R144-k8s-tc-leader.md) | leader=2 |
| [R145-K8s PVC](docs/rounds/R145-k8s-broker-pvc.md) | 重建 15/15 |
| [R146-容器矩阵](docs/rounds/R146-container-sdk-matrix.md) | 14 类通过 |
| [R147-完整Compose](docs/rounds/R147-full-compose.md) | MQ/RocketMQ/Prom |
| [R148-ACL组](docs/rounds/R148-acl-group.md) | topic+group |
| [R149-健康免鉴权](docs/rounds/R149-health-auth.md) | 探针可用 |
| [R150-batch128](docs/rounds/R150-batch128.md) | 30,860 msg/s |
| [R151-batch256](docs/rounds/R151-batch256.md) | 37,095 峰值 |
| [R152-交付冻结](docs/rounds/R152-delivery-freeze.md) | RC 冻结 |
| [R153-1.0计划](docs/22-1.0剩余工作计划.md) | 剩余工作 |
| [R154-交付交接](docs/23-交付交接.md) | 接手说明 |
| [R155-SSD负结果](docs/rounds/R155-ssd-negative.md) | 12–18k |
| [R156-批量合并负结果](docs/rounds/R156-batch-merge-negative.md) | 31–33k |
| [R157-TLS](docs/rounds/R157-tls.md) | 发送/消费通过 |
| [R158-mTLS](docs/rounds/R158-mtls.md) | 服务端校验 |
| [R159-轨迹开关](docs/rounds/R159-trace-toggle.md) | trace-enabled |
| [R160-Java mTLS](docs/rounds/R160-java-mtls.md) | 收发通过 |
| [R161-gRPC](docs/rounds/R161-grpc.md) | Health/Stats |
| [R162-gRPC TLS](docs/rounds/R162-grpc-tls.md) | TLS/mTLS |
| [R163-Raft 租约](docs/rounds/R163-raft-lease.md) | 选主/故障切换 |
| [R164-租约路由](docs/rounds/R164-lease-route.md) | 自动切路由 |
| [R165-容器镜像](docs/rounds/R165-container-image.md) | gRPC mTLS |
| [R166-镜像 SDK](docs/rounds/R166-image-sdk.md) | 7 场景 |
| [R167-完整矩阵](docs/rounds/R167-image-matrix.md) | 14/14 |
| [R168-Inbox](docs/rounds/R168-inbox-container.md) | 15/15 |
| [R169-Grafana](docs/rounds/R169-grafana.md) | 仪表盘 |
| [R170-gRPC TxStatus](docs/rounds/R170-grpc-txstatus.md) | CLI |
| [R171-K8s 双节点](docs/rounds/R171-k8s-multinode.md) | TC 跨节点 |
| [R172-集群内 SDK](docs/rounds/R172-k8s-incluster-sdk.md) | 收发/批量/事务 |
| [R173-RC 验收](docs/rounds/R173-rc-acceptance.md) | RC 关闭 |
| [R175-mq-raft 原型](docs/rounds/R175-mq-raft-prototype.md) | 3 节点日志复制 |
| [R176-broker Raft SEND](docs/rounds/R176-broker-raft-send.md) | 单节点 SEND/Pull |
| [R177-三 broker Raft](docs/rounds/R177-raft-three-broker.md) | 选主/切换 |
| [R178-Raft 自动路由](docs/rounds/R178-raft-auto-route.md) | 自动切路由 |
| [R179-Raft BATCH](docs/rounds/R179-raft-batch.md) | 批量接入 |
| [R180-Raft 追平](docs/rounds/R180-raft-rejoin.md) | follower 重启 |
| [R181-Raft 持久化](docs/rounds/R181-raft-disk-log.md) | 日志落盘 |
| [R182-全集群重启](docs/rounds/R182-raft-full-restart.md) | 恢复/续写 |
| [R183-多数派丢失](docs/rounds/R183-raft-quorum-loss.md) | 写失败/恢复 |
| [R184-K8s 分区](docs/rounds/R184-k8s-partition-netns.md) | pod netns |
| [R186-Raft 批量单条](docs/rounds/R186-raft-batch-single.md) | ~1.9k msg/s |
| [R187-Raft 延时](docs/rounds/R187-raft-delay.md) | 延时投递 |
| [R188-三副本延时](docs/rounds/R188-raft-delay-3node.md) | 3/3 一致 |
| [R190-Raft 事务](docs/rounds/R190-raft-tx.md) | half/commit |
| [R191-三副本事务](docs/rounds/R191-raft-tx-3node.md) | 3/3 一致 |
| [R192-Raft 快照](docs/rounds/R192-raft-snapshot.md) | 快照持久化 |
| [R193-Raft 压缩](docs/rounds/R193-raft-compaction.md) | 日志压缩 |
| [R194-事务 Raft-first](docs/rounds/R194-raft-tx-ordering.md) | 提交顺序 |
| [图表索引](docs/图表索引.md) | PNG/PlantUML |

分阶段报告：`docs/rounds/R01.md` … `R193-raft-compaction.md`（R174/R185/R189 为负结果）；1.0 剩余计划见 [22-1.0剩余工作计划](docs/22-1.0剩余工作计划.md)，接手见 [23-交付交接](docs/23-交付交接.md)；最终验收见 [16-最终验收报告](docs/16-最终验收报告.md)，交付说明见 [21-最终交付说明](docs/21-最终交付说明.md)，交付索引见 [20-交付索引](docs/20-交付索引.md)。

Windows 分发包：[mq-rust-0.9.0-rc.1-win64.zip](dist/mq-rust-0.9.0-rc.1-win64.zip)。

---

## 4. 快速开始（无 Docker）

```powershell
cargo build --release -p mq-namesrv -p mq-broker -p mq-tc

# 单机栈
powershell -ExecutionPolicy Bypass -File deploy/native/start-mq-rust.ps1
# 19877 namesrv / 20921 broker / 19191 tc / 19090 metrics / 19091 admin

# 三节点 TC
powershell -ExecutionPolicy Bypass -File deploy/native/start-tc-cluster.ps1

powershell -ExecutionPolicy Bypass -File deploy/native/stop-mq-rust.ps1
powershell -ExecutionPolicy Bypass -File deploy/native/stop-tc-cluster.ps1
```

手工启动：

```powershell
target\release\mq-namesrv.exe --listen 127.0.0.1:9877 --broker-addr 127.0.0.1:10921
target\release\mq-broker.exe --listen 127.0.0.1:10921 --namesrv 127.0.0.1:9877 `
  --tc-url http://127.0.0.1:19401,http://127.0.0.1:19402,http://127.0.0.1:19403 `
  --tc-reconcile-ms 2000 --inbox-path G:/data/inbox.log
target\release\mq-tc.exe --id 1 --raft-listen 127.0.0.1:19301 --api-listen 127.0.0.1:19401 `
  --peers 1@127.0.0.1:19301,2@127.0.0.1:19302,3@127.0.0.1:19303 --data-dir G:/data/tc-1
```

---

## 5. 测试

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# RocketMQ SDK
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 -Push -Topic MQ_RUST_PUSH
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 -Order -Topic MQ_RUST_ORDER
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 -Inbox -Topic MQ_RUST_INBOX
powershell -ExecutionPolicy Bypass -File tests/compat/run-mqadmin.ps1 127.0.0.1:9877 clusterList
```

---

## 6. 目录

```text
crates/
  mq-common/      错误、配置、Message
  mq-protocol/    Remoting 帧、命令、消息编解码
  mq-store/       CommitLog / ConsumeQueue
  mq-broker/      Broker、事务、CQ、索引、Inbox
  mq-namesrv/     NameServer
  mq-tc/          TC Raft（TCP + WAL + snapshot）
  mq-txn/ mq-client/ mq-observability/
tests/compat/     Java SDK 兼容测试
deploy/           Dockerfile / compose / native / k8s
docs/             设计文档与分阶段报告
diagrams/         PlantUML
```

---

## 7. 已知缺口

1. **性能**：100k 长跑 9,347 TPS，批量 64 条 24,982 msg/s，距 5 万目标远；单 CommitLog 写锁为瓶颈。
2. **容器**：宿主无 Docker/WSL，compose/K8s 未实测。
3. **HA**：异步/同步 ack 主从 + 事件/epoch 栅栏，但无 Raft 共识、无多数派、无逐条精确 ack。
4. **安全**：AccessKey 白名单原型（不校验 Signature）；无 TLS/gRPC。
5. **消费**：Inbox 需应用显式调用；无分布式去重表。
6. **存储**：Retention 已有（条数/字节/TTL），但丢弃时全量重写 CQ；无分段/增量清理。
7. **测试 flake**：Tag 首次订阅偶发超时，R60 复测 5/5 通过，根因未定位。

---

## 8. 后续优先级

| 优先级 | 工作 |
|---|---|
| P0 | 多连接 pipeline / 批量提交，冲 1 万+ TPS |
| P0 | 精确按消息 ack/多数派、Raft 选主 |
| P1 | 容器环境实测（Docker/K8s） |
| P1 | ACL Signature/HMAC、TLS |
| P1 | 分段 CQ / 增量清理 |
| P2 | Inbox 自动拦截/分布式去重表、消息轨迹 |
