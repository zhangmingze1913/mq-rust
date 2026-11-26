# R203 · batch128 调优（负结果）

> 日期：2026-11-26  
> 结果：三副本 batch128 超时更多、吞吐更低，不采用。

---

## 1. 结果

```text
3 副本 Raft，batch128，120s
messages 148,224（三副本一致）
Send Failed 40 / 5,120
MPS 1198–1716
Max RT 2840ms
```

## 2. 对比

```text
batch256 240s: 444,928 / 12 失败 / 1.4–1.8k MPS
batch128 120s: 148,224 / 40 失败 / 1.2–1.7k MPS
batch64  60s:   62,720 / 31 失败 / 0.5–1.0k MPS
```

batch 越小，client_write 次数越多，超时反而增加。

## 3. 结论

- batch128/64 均不采用；
- 保留 batch256；
- 超时根因是单批 apply/持久化 RT 接近 3s，不是 batch 大小；
- 下一步应优化 apply 内部（CQ/索引合并）或调大客户端超时。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
