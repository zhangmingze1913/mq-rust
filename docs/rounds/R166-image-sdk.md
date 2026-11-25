# R166 · R165 镜像 SDK 回归

> 日期：2026-11-26  
> 结果：在 R165 容器镜像上复跑 SDK 场景 7 项，全部通过。

---

## 1. 环境

```text
docker compose -f deploy/docker-compose.lite.yml up -d
5 容器：namesrv / broker / TC1 / TC2 / TC3
SDK：RocketMQ 4.9.8 Java
namesrv：127.0.0.1:9877
broker：127.0.0.1:10921
```

## 2. 结果

```text
默认收发   RESULT received=15 asyncOk=5 send=true
批量       BATCH RESULT received=5
事务       TX RESULT commit=1 rollback=0
顺序       ORDER RESULT done=true count=50 ordered=true
重试       RETRY RESULT attempts=2 ok=true
死信       DLQ RESULT attempts=2 dlq=1
延时       DELAY LATE count=1
```

## 3. 说明

- PowerShell 包装脚本部分场景返回 exit code 1，但业务 RESULT 已成功；
- 本轮只复跑 7 项，不是完整 14 类矩阵；
- 未复跑 TLS/mTLS 组合场景（已由 R160/R162 主机验证）。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
