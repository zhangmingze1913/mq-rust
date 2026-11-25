# R167 · R165 镜像完整 14 类 SDK 矩阵

> 日期：2026-11-26  
> 结果：R165 容器镜像上 14 类 RocketMQ 4.9.8 SDK 场景全部通过。

---

## 1. 环境

```text
docker compose -f deploy/docker-compose.lite.yml up -d
5 容器：namesrv / broker / TC1 / TC2 / TC3
namesrv 127.0.0.1:9877，broker 127.0.0.1:10921
```

## 2. 14 类结果

```text
默认收发    RESULT received=15 asyncOk=5 send=true
Push        PUSH RESULT received=5 expected=5 ok=true
Tag         TAG RESULT received=3 ok=true
Batch       BATCH RESULT received=5
Retry       RETRY RESULT attempts=2 ok=true
Rebalance   REBALANCE RESULT c1=10 c2=6 ok=true
Tx          TX RESULT commit=1 rollback=0
TxTimeout   TX TIMEOUT RESULT visible=0
TxCheck     TX CHECK RESULT checks=1 visible=1
Idem        IDEM RESULT count=1
Dlq         DLQ RESULT attempts=2 dlq=1
Delay       DELAY LATE count=1
StartDeliver START RESULT early=0 late=1
Order       ORDER RESULT done=true count=50 ordered=true
```

## 3. 未覆盖

- Inbox 场景需 admin 9091，Compose 映射到 9192，本轮未跑；
- TLS/mTLS 组合场景在主机验证（R160/R162），未在 Compose 内重复；
- 未做性能压测。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
