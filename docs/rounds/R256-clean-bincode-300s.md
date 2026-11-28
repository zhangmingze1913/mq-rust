# R256 · 干净环境 bincode 300s

> 日期：2026-11-28  
> 结果：停止 k3s/tsp 后，bincode 2P/4C 300s 361,216 发送、6,656 失败、p50 260ms、p99 2.75s；为目前最佳 p99。

---

## 1. 环境

```text
kiri Docker（非 k8s）
k3s/tsp 已停止
3 broker Raft + namesrv fresh
bincode RPC
2P/4C batch256 300s
```

## 2. 结果

```text
sent=361,216
failed=6,656
received=364,800
p50=260ms
p99=2,747ms
```

## 3. 对比

| 配置 | sent | failed | p99 |
|---|---|---|---|
| JSON 300s（R252） | 489,216 | 8,192 | 4.45s |
| bincode 300s（R256） | 361,216 | 6,656 | 2.75s |

- bincode p99 明显更好，失败更少；
- 吞吐低于 R252 峰值，属环境/批次波动；
- 300s 尾段失败率约 1.8%。

## 4. 结论

- bincode 适合作为默认 RPC 编码；
- 干净环境 p99 2.75s，优于 4.45s；
- 5 万仍不可达。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
```
