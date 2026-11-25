# R96 · 自动提升（failover）验证

> 日期：2026-11-25  
> 结果：master 被杀后 slave1 自动提升为 namesrv 路由（epoch 2），新发送 10 条全部成功，slave2 仍为 100。

---

## 1. 拓扑

```text
namesrv 127.0.0.1:9877，admin 9171
master 10921，replica-listen 10931，quorum=2
slave1 10922，advertise 10922，--promote-to 9171
slave2 10923
```

## 2. 结果

```text
发送 100 条：master=100, slave1=100, slave2=100，0 失败

kill master（10921）
slave1 日志：replica promoted in namesrv advertise=127.0.0.1:10922
namesrv admin：{"broker_addr":"127.0.0.1:10922","epoch":2}

新 Producer 发送 10 条：0 失败
slave1 = 110
slave2 = 100（未提升，旧副本）
```

## 3. 说明

- 自动提升由从节点在 master 连接断开后触发；
- namesrv route epoch 递增，客户端新连接路由到 slave1；
- 只有指定 promote-to 的 slave1 提升，避免双主；
- slave2 数据落后 10 条，需重新同步/人工处理。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 无 quorum 选举/任期，提升是“先到先得”；
- 未配置 fence 时旧 master 恢复可能脑裂；
- slave2 未自动追平；
- 无客户端重定向/事务状态迁移验证。
