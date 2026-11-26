# R215 · Raft 心跳/选举调优（负结果，已回退）

> 日期：2026-11-26  
> 结果：将心跳 200ms→1000ms、选举 600–1200ms→3000–6000ms，4 节点启动后无法选出可写 leader，已回退。

---

## 1. 尝试

为降低每 RPC 新建连接造成的端口压力：

```text
heartbeat_interval: 1000
election_timeout_min: 3000
election_timeout_max: 6000
```

## 2. 结果

4 节点启动后：

```text
所有 broker stats messages=0
namesrv route 存在但 BATCH 无响应
A 日志 vote T1-N4 committed，但无稳定 leader
```

多次等待仍无法写入，判定负结果并回退到 200/600/1200。

## 3. 结论

- 单纯降低心跳频率不能解决 4 节点选主/重加入；
- 端口耗尽与 4 节点恢复仍是未解决问题；
- 3 节点既有配置保持不变。

## 4. 质量门禁

```text
回退后 cargo fmt / clippy / test 通过（87 项）
```
