# R169 · Grafana 数据源与仪表盘

> 日期：2026-11-26  
> 结果：Grafana UI 可登录；Prometheus 数据源与 MQ-rust 仪表盘自动加载。

---

## 1. 配置

新增：

```text
deploy/grafana/provisioning/datasources/prometheus.yml
deploy/grafana/provisioning/dashboards/default.yml
deploy/grafana/dashboards/mq-rust.json
```

Compose 挂载：

```yaml
- ./grafana/provisioning:/etc/grafana/provisioning:ro
- ./grafana/dashboards:/var/lib/grafana/dashboards:ro
- grafana-data:/var/lib/grafana
```

## 2. 验证

```text
GET /api/health
{"database":"ok","version":"13.3.2"}

GET /api/datasources
[{"uid":"prometheus","name":"Prometheus","url":"http://prometheus:9090","isDefault":true}]

GET /api/search?query=MQ
[{"title":"MQ-rust Broker","uid":"mq-rust","type":"dash-db"}]
```

仪表盘面板：

- Send TPS
- Pull TPS
- Topics
- Pending half
- CommitLog bytes

## 3. 启动说明

首次启动 Grafana 迁移在 HDD 上约 5 分钟；迁移完成后若 HTTP 空响应，`docker restart deploy-grafana-1` 可恢复。

## 4. 限制

- 本轮只启动 grafana（含依赖 prometheus），未启动 broker，面板暂无数据；
- 未截图；
- 默认 admin/admin 仅本地环境。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
