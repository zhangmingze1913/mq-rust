# R173 · RC 验收关闭

> 日期：2026-11-26  
> 结果：用户接受 RC 口径（quorum HA + batch256 37k），5 万与 Raft 消息日志明确排除；本次交付关闭。

---

## 1. 验收范围

- 功能：普通/顺序/事务/延时/批量/死信/重试/Inbox；
- SDK：RocketMQ 4.9.8 Java 15/15 场景；
- 安全：remoting/gRPC TLS + mTLS、ACL topic+group；
- HA：quorum + Raft 租约选主 + 自动路由 + fence；
- 容器：Compose 15/15；K8s 双节点 + 集群内 SDK；
- 可观测：Prometheus + Grafana 数据源/仪表盘；
- 性能：batch256 **37,095 msg/s**；
- 质量：83 项 Rust 测试。

## 2. 明确排除

- 5 万 TPS（现有 2C/4T + HDD/小 SSD 不可达）；
- Raft 消息日志复制（保留设计 docs/25）；
- K8s 网络分区故障注入；
- 生产 1.0 声明。

## 3. 交付

```text
tag: v0.9.0-rc.1
分发包 SHA256 见 docs/20-交付索引.md
```

## 4. 后续可选

新硬件冲 5 万、Raft 日志改造、网络分区演练。
