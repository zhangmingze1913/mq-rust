# 兼容性测试（R1 已完成）

目标：用 **RocketMQ 官方 Java SDK** 验证 MQ-rust 的协议兼容性。

## 已通过（R1）

| 编号 | 客户端 | 场景 | 结果 |
|---|---|---|---|
| C01 | `DefaultMQProducer` | 同步发送 5 条 | ✅ `SEND_OK` |
| C02 | `DefaultMQProducer` | 异步发送 5 条 | ✅ `asyncOk=5` |
| C03 | `DefaultMQProducer` | 单向发送 5 条 | ✅ 无异常 |
| C04 | `DefaultMQPullConsumer` | 8 queue 拉取 15 条 | ✅ body/tags 正确，`nextBeginOffset` 正确 |
| C05 | `DefaultMQPullConsumer` | max/min/update/fetch offset | ✅ |
| C06 | Broker 重启 | 只拉取已有消息 | ✅ 15/15 |
| C07 | `benchmark.Producer` | ROCKETMQ 二进制 header | ✅ 2000 条，失败 0 |
| C08 | `DefaultMQPushConsumer` | 心跳/Rebalance/长轮询/消费 | ✅ 5/5（R2 第一阶段） |
| C09 | `DefaultMQPushConsumer` | Tag 过滤（TagA vs TagB） | ✅ 3/3（R2） |
| C10 | `DefaultMQProducer.send(Collection)` | 批量消息 | ✅ 5/5（R2） |
| C11 | `DefaultMQPushConsumer` | 失败重投（RECONSUME_LATER） | ✅ 2 次投递（R2） |
| C12 | 2 个 `DefaultMQPushConsumer` | 同组 Rebalance | ✅ c1=5、c2=5（脚本复跑） |
| C13 | `mqadmin clusterList` | 官方工具访问 NameServer/Broker | ✅ exit 0 |
| C14 | `TransactionMQProducer` | commit + rollback | ✅ commit=1、rollback=0 |
| C15 | `TransactionMQProducer` | UNKNOW → 超时回滚 | ✅ 10s 后 visible=0 |
| C16 | `TransactionMQProducer` | UNKNOW → check-back → commit | ✅ checks=1、visible=1 |
| C17 | `DefaultMQProducer` | 同 `UNIQ_KEY` 重复发送 | ✅ count=1 |
| C18 | 事务审计 | JSONL prepare/commit/rollback | ✅ 4 行事件 |
| C19 | DLQ | max=1 → `%DLQ%group` | ✅ attempts=2、dlq=1 |
| C20 | SQL92 | `price > 100` | ✅ count=1（price=150） |
| C21 | `mqadmin topicList` | 官方工具列出 Topic | ✅ MQ_RUST_TOPICLIST、TBW102 |
| C22 | `mqadmin consumerProgress` | 消费进度/堆积 | ✅ offsetTable、Diff Total |
| C23 | 延时消息 | level 2=5s | ✅ early=0、late=1 |
| C24 | `__STARTDELIVERTIME` | 任意 10s 时间戳 | ✅ early=0、late=1 |
| C25 | 顺序消息 | 单队列 50 条 | ✅ ordered=true |
| C26 | 事务人工 commit | `POST /tx/{id}/commit` | ✅ HTTP 200、visible=1 |

## 运行

前置：MQ-rust NameServer/Broker 已启动。

```powershell
# 发送 + 拉取
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Namesrv 127.0.0.1:9877 -Topic MQ_RUST_SMOKE

# Push 消费
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Push -Topic MQ_RUST_PUSH

# Tag 过滤
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Tag -Topic MQ_RUST_TAG

# 批量消息
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Batch -Topic MQ_RUST_BATCH

# 失败重投
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Retry -Topic MQ_RUST_RETRY

# 多消费者 Rebalance
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Rebalance -Topic MQ_RUST_REBALANCE

# 事务消息 commit/rollback
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Tx -Topic MQ_RUST_TX

# 事务超时回滚
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -TxTimeout -Topic MQ_RUST_TX_TIMEOUT

# 事务 check-back
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -TxCheck -Topic MQ_RUST_TX_CHECK

# DLQ
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Dlq -Topic MQ_RUST_DLQ

# SQL92（Push）
java -cp "$out;$cp" MqRustSql92PushSmoke 127.0.0.1:9877 MQ_RUST_SQL92_PUSH

# 幂等生产
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Idem -Topic MQ_RUST_IDEMPOTENT

# 审计日志（Broker 启动参数）
# --audit-path G:/MQ_soft/mq-rust-data/audit.log
Get-Content G:/MQ_soft/mq-rust-data/audit.log

# 延时消息
java -cp "$out;$cp" MqRustDelaySmoke 127.0.0.1:9877 MQ_RUST_DELAY

# 任意时间戳延时
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -StartDeliver -Topic MQ_RUST_START_DELIVER

# 顺序消息
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -Order -Topic MQ_RUST_ORDER

# 事务人工 commit
java -cp "$out;$cp" MqRustTxManualSmoke 127.0.0.1:9877 MQ_RUST_TX_MANUAL http://127.0.0.1:9091

# mqadmin topicList
powershell -ExecutionPolicy Bypass -File tests/compat/run-mqadmin.ps1 `
  127.0.0.1:9877 topicList

# mqadmin consumerProgress
powershell -ExecutionPolicy Bypass -File tests/compat/run-mqadmin.ps1 `
  127.0.0.1:9877 consumerProgress -g mq_rust_sql92_push_group
powershell -ExecutionPolicy Bypass -File tests/compat/run-mqadmin.ps1

# 重启后只拉取（先杀 Broker、用同一 store-root 重启）
powershell -ExecutionPolicy Bypass -File tests/compat/run-smoke.ps1 `
  -PullOnly -Expected 15 -Topic MQ_RUST_PERSIST
```

## 文件

- `src/MqRustCompatSmoke.java`
- `src/MqRustPullOnly.java`
- `src/MqRustPushSmoke.java`
- `src/MqRustTagSmoke.java`
- `src/MqRustBatchSmoke.java`
- `src/MqRustRetrySmoke.java`
- `src/MqRustRebalanceSmoke.java`
- `src/MqRustTxSmoke.java`
- `src/MqRustTxTimeoutSmoke.java`
- `src/MqRustTxCheckBackSmoke.java`
- `src/MqRustIdempotentSmoke.java`
- `src/MqRustSql92PushSmoke.java`
- `src/MqRustDelaySmoke.java`
- `src/MqRustStartDeliverSmoke.java`
- `src/MqRustOrderSmoke.java`
- `src/MqRustTxManualSmoke.java`
- `run-mqadmin.ps1`
- `run-smoke.ps1`

## 下一轮（R2）

- `DefaultMQPushConsumer`（心跳、Rebalance、长轮询）
- 批量消息、Tag/SQL92 过滤、重试/DLQ
- `mqadmin` 常用命令
