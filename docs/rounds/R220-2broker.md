# R220 · 2 broker 集群验证

> 日期：2026-11-27  
> 结果：2 节点 quorum 2；单节点故障时不可写，恢复后继续写入，两副本一致。

---

## 1. 步骤

```text
2 broker Raft（quorum 2）
BATCH A 5
kill leader B
BATCH B -> 无 quorum，挂起/失败
重启 B
BATCH C 5
```

## 2. 结果

```text
初始 BATCH 5/5
无 quorum 时不可写（符合预期）
恢复后 BATCH 5/5
A/B messages = 10，完全一致
```

## 3. 结论

- 2 broker 可运行，但无容错（任一节点故障即不可写）；
- 适合开发/测试，不适合生产 HA；
- 3/4 broker 已验证可容错。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（88 项）
```
