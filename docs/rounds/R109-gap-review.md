# R109 · 1.0 差距复核

> 日期：2026-11-25  
> 结果：形成 [docs/19-1.0差距清单.md](../19-1.0差距清单.md)，量化 10 项差距与建议工作量。

---

## 1. 差距

| 项 | 当前 | 阻塞 |
|---|---|---|
| 5 万 TPS | 8,438 | HDD/4 核/单写 |
| 容器 | 未实测 | 无运行时 |
| 严格 HA | quorum+提升+fence | 无 Raft 任期/投票 |
| TLS/gRPC | 无 | 依赖/工程量 |
| group ACL | 仅 topic | schema |
| 轨迹持久化 | 内存 | 未落盘 |
| 精确段回收 | 整段 | 段内旧数据 |
| 多副本事务 | 单机 | 未验证迁移 |

## 2. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 3. 结论

功能/SDK 兼容 RC；生产 1.0 仍需性能、容器、Raft、安全四类工作。
