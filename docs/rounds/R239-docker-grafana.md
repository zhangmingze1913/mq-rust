# R239 · Docker Compose + Grafana 复验

> 日期：2026-11-27  
> 结果：完整 Compose 栈启动；SDK 15/15；Prometheus 的 mq-rust 目标 up；Grafana 10.4.2 健康、数据源可用、仪表盘已加载。

---

## 1. 修复

- `prometheus.yml` 目标由 `mq-rust:9090` 改为 `mq-rust-broker:9090`；
- Grafana 由 `latest` 固定到 `10.4.2`（latest 迁移在本机过慢，无法及时启动）。

## 2. 验证

```text
docker compose up -d
Grafana /api/health: {"database":"ok","version":"10.4.2"}
Prometheus /api/v1/targets: mq-rust-broker:9090 health=up
Grafana datasource Prometheus health: OK
Grafana dashboard: MQ-rust Broker
SDK: RESULT received=15 asyncOk=5 send=true
```

## 3. 端口

```text
9877 namesrv
10921 broker
9092 Prometheus（容器 9090）
3000 Grafana（admin/admin）
19401-19403 TC
```

## 4. 边界

- 单机 Compose，非多机；
- RocketMQ 5.3.4 容器同时运行，其 /metrics 未启用；
- Grafana 镜像固定 10.4.2。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（91 项）
Docker Compose SDK 15/15
Grafana/Prometheus 检查通过
```
