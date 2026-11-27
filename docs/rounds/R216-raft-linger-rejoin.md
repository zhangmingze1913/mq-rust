# R216 · Raft 连接 linger=0 修复 4 节点重加入

> 日期：2026-11-27  
> 结果：客户端/服务端 TCP 设置 `SO_LINGER=0`，避免 TIME_WAIT 端口耗尽；4 节点 kill/重启后恢复写入。

---

## 1. 变更

- Raft 客户端 connect 后 `set_linger(0)`；
- 服务端保持正常 FIN（避免 RST 丢弃响应）；
- 保持每 RPC 新建连接，不做长连接复用（R214 已回退）。

## 2. 4 节点验证

```text
4 broker（quorum 3）
初始 BATCH 5/5
kill follower B -> BATCH 5/5
重启 B -> BATCH 5/5
```

路由切到新 leader，发送恢复。

## 3. 发现的新问题

重启 B 后统计：

```text
A/C: 15
B:   20（MQ_R25A 重复 5 条）
```

B 重启后重放 Raft 日志，BrokerStore 去重未完全阻止重复 append，出现状态重复。  
该问题与连接/选主无关，是状态机重放幂等缺陷。

## 4. 结论

- `SO_LINGER=0` 解决端口耗尽和 4 节点重加入；
- 4 节点可恢复写入；
- 重启后消息重复未解决。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（87 项）
```
