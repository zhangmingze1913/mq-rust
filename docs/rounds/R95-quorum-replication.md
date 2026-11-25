# R95 · 多副本 Quorum 同步复制

> 日期：2026-11-25  
> 结果：1 主 2 从、`--replica-quorum 2`；100 条三副本一致；停 1 从后 3 条发送失败；恢复后 113 条一致。

---

## 1. 机制

- 新增 `--replica-quorum`（默认 1）；
- 每个 slave ack 累加全局 `replica_ack_events`；
- 同步复制等待 `ack_events >= before + quorum`；
- 超时返回失败，已写入的消息由 msgId 去重兜底。

## 2. 验证

```text
master: --sync-replicate --replica-quorum 2 --replica-timeout-ms 2000
slave1/slave2: --replica-of ...:10931

发送 100 条：
  master=100, slave1=100, slave2=100，0 失败

停 slave2 后发送 3 条：
  Send Failed=3（等待 quorum 超时）

重启 slave2：
  master=113, slave1=113, slave2=113
  再发 10 条 0 失败
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 无自动选主/任期，仍为人工提升；
- quorum 超时后消息可能已落单副本，依赖去重；
- 同步 quorum 吞吐低；
- 未实现 quorum 读/脑裂 fencing 的完整协议。
