# R219 · 三副本 batch512 长跑（负结果）

> 日期：2026-11-27  
> 结果：三副本 batch512/4 线程 400s 写入约 51.9 万条，256 次超时，吞吐低于 batch256。

---

## 1. 配置

```text
3 副本 Raft
batch512 / 256B / 4 threads
400s
retain 1,200,000
```

## 2. 结果

```text
messages: 519,680 / 519,168 / 519,680
Send Failed: 256
MPS ~2669–3013（尾段 0）
Max RT 2937ms
```

## 3. 对比

```text
batch256 4 线程 120s: 226,816 / 0 失败
batch512 4 线程 400s: 519,680 / 256 失败
```

batch512 单批 apply 时间更长，超时更多，不采用。

## 4. 结论

- batch256 仍是三副本最佳；
- 三副本 1M 在当前 HDD/4 线程下不可达；
- 继续需 NVMe/更多核或分片集群。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（88 项）
```
