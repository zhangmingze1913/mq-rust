# R254 · Raft RPC bincode 编码

> 日期：2026-11-28  
> 结果：Raft RPC 改为 bincode（带 MQB1 魔数 + JSON 回退）；60s 全链路 107,520/0 失败、p99 791ms；300s 137,984/18,688、p99 17.5s。

---

## 1. 变更

- `encode_rpc`：4 字节 `MQB1` + bincode；
- `decode_rpc`：识别魔数用 bincode，否则按 JSON 解析；
- 客户端/服务端 RPC 全部使用新编码；
- 兼容旧节点 JSON 请求。

## 2. 60s 结果

```text
2P/4C 60s: sent=107,520 failed=0 received=107,520
p50=251ms p99=791ms
```

对比 JSON 60s：157,440/0，p50 348ms，p99 911ms。延迟更好，吞吐受环境波动影响。

## 3. 300s 结果

```text
2P/4C 300s: sent=137,984 failed=18,688
p50=442ms p99=17.5s
```

低于 JSON 最佳 489,216/8,192；当前 Kiri 内 k3s 控制面/其他容器同时运行，环境不可比。

## 4. 结论

- bincode 降低 RPC 体积/序列化开销，60s 延迟改善；
- 300s 未显示吞吐提升，需隔离环境 A/B；
- 保留 bincode（含 JSON 兼容回退），后续在干净环境复测。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
```
