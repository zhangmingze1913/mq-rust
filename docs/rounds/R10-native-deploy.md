# R10 · 原生等价部署与最终状态

> 日期：2026-11-24  
> 环境限制：宿主无 Docker / Podman / WSL，容器路径不可实测。  
> 结果：**原生进程等价栈启动/冒烟/停止通过**；Dockerfile/compose 已更新为 NameServer + Broker + TC 三服务。

---

## 1. 原生栈验证

```text
port 19877: UP   # NameServer
port 20921: UP   # Broker
port 19191: UP   # TC
port 19090: UP   # metrics
port 19091: UP   # admin

RESULT received=15 asyncOk=5 send=true
TX RESULT commit=1 rollback=0
```

脚本：

```text
deploy/native/start-mq-rust.ps1
deploy/native/stop-mq-rust.ps1
deploy/native/start-tc-cluster.ps1
deploy/native/stop-tc-cluster.ps1
```

最终复验（R31 后）：

```text
port 19877/20921/19191/19090/19091 UP
RESULT received=15 asyncOk=5 send=true
TX RESULT commit=1 rollback=0
INBOX RESULT delivered=3 processed=1
```

TC 通过 `--api-listen 127.0.0.1:19191` 启动（脚本已修正）。

## 2. 容器资产

- `deploy/Dockerfile`：构建 `mq-broker`、`mq-namesrv`、`mq-tc`；
- `deploy/docker-compose.yml`：
  - `mq-rust-namesrv`
  - `mq-rust-broker`（`--tc-url http://mq-rust-tc-1:9191,...tc-2,...tc-3`）
  - `mq-rust-tc-1/2/3`（TCP Raft，`--data-dir /data/tc`）
  - RocketMQ 4.9.8 / Prometheus / Grafana 基线；
- 未实测：宿主无容器运行时。
- `deploy/k8s/mq-rust.yaml`：NameServer/Broker/TC StatefulSet（3 副本），YAML 解析通过，未在集群运行。

## 3. 最终能力矩阵

| 维度 | 状态 |
|---|---|
| 普通/批量/Tag/SQL92/顺序/延时/事务/DLQ | ✅ |
| mqadmin clusterList/topicList/consumerProgress | ✅ |
| Half WAL / 重试 WAL / delay WAL / 审计 | ✅ |
| TC 单节点 + 三节点进程内 Raft + 状态 WAL | ✅ |
| Broker↔TC + 持久化 outbox | ✅ |
| release 性能 | ✅ 7,127 TPS（8 线程，开发机） |
| 跨进程 TCP Raft | ⬜ |
| 5 万+ TPS | ⬜ |
| Docker/K8s 实测 | ⬜（环境缺失） |
| ConsumeQueue 独立索引 / Inbox / ACL/TLS/gRPC | ⬜ |

## 4. 复现

```powershell
cargo build --release -p mq-namesrv -p mq-broker -p mq-tc
powershell -ExecutionPolicy Bypass -File deploy/native/start-mq-rust.ps1
# smoke...
powershell -ExecutionPolicy Bypass -File deploy/native/stop-mq-rust.ps1
```

## 5. 结论

MQ-rust 已具备**可运行、可验收的单机事务消息中间件**形态：  
RocketMQ 4.9.8 SDK 兼容、事务/延时/顺序/DLQ/SQL92、TC Raft 状态机、持久化 outbox 与原生部署脚本。  
距离“生产 1.0”仍有性能、跨进程 Raft/HA、容器实测与安全等缺口，不能宣称目标全部达成。
