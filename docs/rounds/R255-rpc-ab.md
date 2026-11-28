# R255 · JSON vs bincode RPC A/B

> 日期：2026-11-28  
> 结果：同一镜像通过 `MQ_RAFT_RPC_JSON=1` 切换；干净 Kiri 环境 60s：bincode 107,520/0、p99 791ms；JSON 100,864/0、p99 1075ms。

---

## 1. 方法

```text
停止 kiri-k8s / tsp-kiri，释放 CPU
同一 mq-rust 镜像
bincode：默认
JSON：MQ_RAFT_RPC_JSON=1
每次重启 3 broker fresh
2P/4C 60s batch256
```

## 2. 结果

| 编码 | sent | failed | received | p50 | p99 |
|---|---|---|---|---|---|
| bincode | 107,520 | 0 | 107,520 | 251ms | 791ms |
| JSON | 100,864 | 0 | 100,864 | 212ms | 1075ms |

## 3. 结论

- 吞吐相近（~1.7k msg/s）；
- bincode p99 约低 26%；
- JSON p50 略低，差异在噪声范围；
- 保留 bincode + JSON 回退，兼容旧节点。

## 4. 附加

- 新增 `MQ_RAFT_RPC_JSON=1` 环境开关，便于 A/B 和回滚；
- 干净环境比 k3s 同时运行稳定。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
```
