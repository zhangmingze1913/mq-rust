# R112 · 迭代收口（160/160）

> 日期：2026-11-25  
> 结论：**未完成生产 1.0**；交付 0.9.0-rc.1，迭代轮次到顶。

---

## 1. 最终状态

```text
tag: v0.9.0-rc.1
Rust: 79 tests
Java: 23 scenarios
zip SHA256: BE2E631F9550A70AF2EB1DF2A21909996569B100AF4278459C09856B6EAD3309
1M: 8,438 TPS, 0 failed
batch64: 24,982 msg/s
consume: 11,130 TPS
```

## 2. 已完成

- RocketMQ 4.9.8 协议与 Java SDK 主要模式；
- 普通/顺序/事务/延时/批量/死信；
- CommitLog/CQ/索引/位点分段与回收；
- Retention、崩溃恢复、half/定时重启；
- 同步复制、quorum、自动提升、旧主 fence；
- ACL Signature/Topic/IP/CIDR/热更新；
- 轨迹、指标、Admin、分发包、运维手册。

## 3. 未完成（阻塞）

| 项 | 原因 |
|---|---|
| 容器/K8s 实测 | 环境无 Docker/Podman/WSL 发行版 |
| 5 万 TPS | HDD/4 核/单写，实测 8.4k |
| 严格 Raft 选举 | 无任期/投票实现 |
| TLS/gRPC/group ACL | 未实现 |
| 轨迹持久化/精确回收 | 未完成 |

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 说明

- 不声明生产 1.0；
- 不将容器/K8s 资产标为已验证；
- 后续按 [19-1.0差距清单](../19-1.0差距清单.md) 推进。
