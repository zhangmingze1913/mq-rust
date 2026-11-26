# R202 · 三副本 240s 持续负载

> 日期：2026-11-26  
> 结果：444,928 条三副本最终一致；12 次发送超时；follower 结束后 25s 内追平。

---

## 1. 配置

```text
3 broker Raft，batch256，240s
retain-messages 1,000,000
```

## 2. 结果

```text
A 期间: 352,256
B/C:    444,928
结束后 25s: A/B/C = 444,928

Send Failed: 12 / 3072 messages
MPS 1421–1772
Average RT ~1.15–1.36s
Max RT 2893ms（接近 3s 超时）
```

## 3. 结论

- 批量 apply 后长时间负载仍偶发超时（约 3 次/分钟）；
- follower 会短暂落后，负载停止后自动追平；
- 三副本最终一致，无数据丢失；
- 超时主因是 batch256 单批 RT 接近客户端 3s 超时。

## 4. 下一步

- 降低 batch（128）或调大客户端超时；
- 减少单批 apply 时间；
- 1M 持续长跑。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
