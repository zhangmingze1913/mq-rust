# R33 · 交付一致性检查

> 日期：2026-11-25  
> 目的：在 79/80 轮做最终交付物一致性核对。

---

## 1. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
```

## 2. 文档与部署资产

```text
Markdown 乱码          0
Markdown 失效链接      0
docker-compose.yml     YAML 1 个文档（9 services）
k8s/mq-rust.yaml       YAML 7 个资源
```

## 3. 发布二进制

```text
mq-broker.exe   2.15 MB
mq-namesrv.exe  1.50 MB
mq-tc.exe       2.45 MB
```

## 4. 运行环境

- 无遗留 `mq-broker`/`mq-namesrv`/`mq-tc` 进程；
- 无 9877/10921/9191/19877/20921/19191 监听。

## 5. 核心交付

| 类别 | 状态 |
|---|---|
| RocketMQ 4.9.8 SDK 功能 | ✅ |
| 事务/TC/outbox/reconcile | ✅ |
| CQ/索引/位点/Inbox | ✅ |
| TC 三节点 TCP Raft + 持久化/快照/压缩 | ✅ |
| Broker 主从 + 重放 + 自动切换 + ack | ✅（最小防脑裂） |
| 原生栈 5 端口 + SDK/事务/Inbox | ✅ |
| K8s/compose 资产 | 🟡 未实测 |
| 性能 100k 长跑 | ✅ 9,347 TPS |
| 5 万 TPS / 严格 HA / 安全 | ⬜ |

## 6. 结论

交付物一致、可构建、可回归；项目**未达到生产 1.0**，目标未全部达成。  
goal 保持 active，最终结论在 R34。
