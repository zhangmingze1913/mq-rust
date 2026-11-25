# R147 · 完整 Compose 栈

> 日期：2026-11-25  
> 结果：MQ-rust + RocketMQ 5.3.4 + Prometheus 同栈运行；Grafana 容器 Up 但宿主端点未验证。

---

## 1. 启动

```powershell
docker compose -f deploy/docker-compose.yml up -d
```

容器：

```text
deploy-mq-rust-broker-1    Up
deploy-mq-rust-namesrv-1   Up
deploy-mq-rust-tc-1/2/3    Up
deploy-namesrv-1           Up (RocketMQ)
deploy-rocketmq-broker-1   Up (RocketMQ)
deploy-prometheus-1        Up
deploy-grafana-1           Up
```

## 2. 验证

```text
MQ-rust ACL SEND_OK 127.0.0.1:9877
/stats (9192): messages=135 topics=24
Prometheus 9092: HTTP 200
RocketMQ 9876/10911、Grafana 3000 已映射
```

Grafana `/login` 返回连接被关闭，未确认 UI 可用。

## 3. 端口

- MQ-rust admin 宿主映射 **9192:9091**（9091 被其他容器占用）；
- Prometheus 宿主 9092→9090；
- Grafana 3000。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 5. 限制

- Grafana UI 未验证；
- RocketMQ 5.3.4 未跑 Java SDK 对照；
- 同宿主端口冲突较多，生产需独立主机/端口规划。
