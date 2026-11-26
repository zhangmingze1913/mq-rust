# R213 · 单节点 Raft 194 万长跑

> 日期：2026-11-26  
> 结果：单节点 Raft batch256/4 线程 350s 写入 1,942,784 条 256B 消息，40 次超时。

---

## 1. 配置

```text
单节点 Raft
batch256 / 256B / 4 threads
350s
retain-messages 1,100,000
```

## 2. 结果

```text
messages: 1,942,784
body_bytes: 497,352,704
commitlog_bytes: 808,198,144
Send Failed: 40（10,240 条）
MPS 1028–4730（含尾段）
Average RT ~0.15–0.29s
Max RT 2831ms
```

## 3. 结论

- 单节点 Raft 可完成 1M+；
- 尾段仍出现 40 次超时（主机 I/O + 客户端 3s）；
- 三副本 1M 仍未完成（最大 633k/500s）；
- 5 万 TPS 仍受硬件限制。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（87 项）
```
