# R27 · Broker 主从复制（手动切换）

> 日期：2026-11-25  
> 状态：**Master 追加消息后通过 TCP 流式复制到 Slave；kill master 后 Slave 可继续收发（手动切换）**。

---

## 1. 拓扑

```text
namesrv 9877 → master broker 10921
namesrv 9878 → slave  broker 10931
master  --replica-listen 127.0.0.1:10930
slave   --replica-of 127.0.0.1:10930
```

## 2. 验证

写入 master（15 条）：

```text
RESULT received=15 asyncOk=5 send=true
```

从 slave namesrv 拉取：

```text
PULL_ONLY received=15 expected=15
```

kill master 后，直接向 slave 发送并拉取：

```text
RESULT received=15 asyncOk=5 send=true
PULL_ONLY received=15 expected=15
```

## 3. 实现

- `BrokerStore.replicas` + `replica_tx`（异步 channel）；
- `append` 成功后把帧 `try/send` 到 writer 线程，**不阻塞写路径**；
- writer 线程负责 Add/Frame 与 200ms 写超时，慢 Slave 自动剔除；
- master `--replica-listen` 接受连接并交给 writer；
- slave `--replica-of` 循环连接/读取，`store.append` 落盘（CommitLog/CQ/索引/位点）。

实测：master 15 条 → slave 拉取 15 条。

### 断连重放

- Slave 断开期间 master 继续写；
- Slave 重连时 master `replay_to` 重放当前全部消息；
- Slave `append` 按 msg_id 去重，避免重复。

验证：

```text
第一批 15 条 → slave 在线
停止 slave
第二批 15 条（master 共 30）
重启 slave → PULL_ONLY received=30 expected=30
```

### 自动切换

- NameServer 增加 `--admin-listen 9880` 与 `POST /broker/{addr}` 路由更新；
- Slave 配置 `--promote-to 127.0.0.1:9880 --advertise-addr 127.0.0.1:10931`；
- Master 断开后 Slave 自动把 NameServer 路由切到自己。

验证：

```text
kill master(10921)
GET :9880/ -> {"broker_addr":"127.0.0.1:10931"}
向 9877 发送 15 条 -> RESULT received=30（含复制历史）
PULL_ONLY received=30 expected=30
```

防脑裂最小措施：
- Promote 前先 GET namesrv，若已指向自己则跳过；
- 成功 promote 后停止从旧 master 复制并退出复制线程，避免双写分叉。

### 复制确认

- Slave 每 append 一条，向 Master 回写 `u64` 累计条数；
- Master `GET /replica` 返回 `{"acked":N}`；
- 实测 15 条消息：`{"acked":15}`。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       59 项通过
```

## 5. 限制

| 限制 | 计划 |
|---|---|
| 手动切换，无自动选主 | ✅ 自动路由切换；仅最小防脑裂 |
| 无复制 ack/校验 | 🟡 已有累计 ack；无逐条校验 |
| Slave 不自动接管 namesrv | ✅ `--promote-to` |

## 6. 结论

Broker 具备基础主从复制与手动切换能力，但**不是生产级 HA**；自动故障转移、复制确认与背压仍待实现。
