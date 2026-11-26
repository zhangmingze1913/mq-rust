# R183 · Raft 多数派丢失与恢复

> 日期：2026-11-26  
> 结果：3 节点中停掉 2 个后写入失败且状态不变；恢复 2 个节点后写入成功并三副本一致。

---

## 1. 步骤

```text
3 broker 正常，M1 已提交（messages=1）
kill A、C，仅剩 leader B
mqadmin send M2
```

## 2. 多数派丢失

```text
send 抛出 RemotingSendRequestException
B /stats messages=1
```

leader 无法在 1/3 节点上提交，写入未生效。

## 3. 恢复

重启 A、C：

```text
mqadmin send M3
SEND_OK

A/B/C /stats messages=2
```

A/C 从 B 的日志追平，写入恢复。

## 4. 结论

- Raft 提交要求多数派；
- 少数派 leader 不会单机提交；
- 节点恢复后自动追平；
- 未做真正网络分区（Windows loopback 防火墙无法隔离本机环回），本轮为**多数派丢失**故障注入。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（84 项）
```
