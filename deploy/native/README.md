# MQ-rust 原生进程部署（无 Docker）

本目录提供 Windows 原生等价脚本，用于宿主没有 Docker/WSL 时的启动、停止与冒烟验证。

## 端口

| 服务 | 地址 |
|---|---|
| NameServer | `127.0.0.1:19877` |
| Broker | `127.0.0.1:20921` |
| Metrics | `127.0.0.1:19090` |
| Admin | `127.0.0.1:19091` |
| TC | `127.0.0.1:19191` |

## 命令

```powershell
# 构建
cargo build --release -p mq-namesrv -p mq-broker -p mq-tc

# 启动（数据写到 G:\MQ_soft\mq-rust-native）
powershell -ExecutionPolicy Bypass -File deploy/native/start-mq-rust.ps1

# 冒烟
java -cp "$out;$cp" MqRustCompatSmoke 127.0.0.1:19877 MQ_NATIVE_SMOKE
java -cp "$out;$cp" MqRustTxSmoke 127.0.0.1:19877 MQ_NATIVE_TX_C MQ_NATIVE_TX_R

# 停止
powershell -ExecutionPolicy Bypass -File deploy/native/stop-mq-rust.ps1
```

## 验证结果

```text
port 19877: UP
port 20921: UP
port 19191: UP
port 19090: UP
port 19091: UP

RESULT received=15 asyncOk=5 send=true
TX RESULT commit=1 rollback=0
```

## 说明

- 脚本不依赖 Docker，适合当前 Windows 开发机。
- Docker 资产见 `deploy/Dockerfile` 与 `deploy/docker-compose.yml`；宿主无 Docker，容器路径未实测。
- TC 的监听地址通过 `--api-listen` 指定（脚本已用 19191）。

## 三节点 TC 集群

```powershell
powershell -ExecutionPolicy Bypass -File deploy/native/start-tc-cluster.ps1
powershell -ExecutionPolicy Bypass -File deploy/native/stop-tc-cluster.ps1
```

端口：Raft 19301-19303，API 19401-19403；数据目录 `G:\MQ_soft\mq-rust-tc-cluster\node-*`。

