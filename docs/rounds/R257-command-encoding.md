# R257 · RaftCommand base64+bincode 编码

> 日期：2026-11-28  
> 结果：Raft 日志 status 从 JSON 改为 `B64:` + tag + bincode；60s p99 从 791ms 降到 549ms；300s 尾段仍不稳定。

---

## 1. 变更

- `encode_command`：1 字节 tag（0 Batch / 1 Single）+ bincode + base64，前缀 `B64:`；
- `decode_command`：支持 `B64:` 与旧 JSON；
- 仅影响 Raft entry `status`，不影响 RPC 的 bincode 编码。

## 2. 60s

```text
2P/4C 60s: sent=111,872 failed=0 received=111,872
p50=252ms p99=549ms
```

对比之前 bincode RPC：p99 791ms；JSON：1075ms。

## 3. 300s

```text
sent=120,320 failed=18,688 received=133,120
p50=329ms p99=63.8s
```

尾段仍出现严重退化，与 CPU/单 leader 写入积压相关，非编码本身。

## 4. 结论

- 60s 编码收益明确，p99 降低约 30%；
- 300s 尾段需要从写入背压/超时/资源隔离解决；
- 保留 base64+bincode，兼容旧 JSON。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（94 项）
```
