# R244 · K8s 丢包与网络分区

> 日期：2026-11-27  
> 结果：broker-0 30% 丢包、100% 分区期间，3 副本仍完成 15/15 写入；恢复后四副本均 75 条一致，0 重启。

---

## 1. 步骤

```text
broker-0 tc netem loss 30% -> SDK 15/15
broker-0 tc netem loss 100% -> SDK 15/15（3 副本 quorum）
tc qdisc del dev eth0 root
```

## 2. 结果

```text
MQ_LOSS received=15
MQ_PART received=15
broker-0..3 messages 全为 75
topics: 5 × 15
0 重启
```

分区期间 broker-0 被隔离，其余 3 副本继续提交；恢复后 broker-0 追平。

## 3. 说明

- 100% 丢包等价网络分区；
- quorum 3/4 保证可用；
- 恢复后日志追平，无消息丢失。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
K8s 丢包/分区 15/15，四副本 75 条一致
```
