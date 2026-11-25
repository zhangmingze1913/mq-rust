# R146 · 容器内 SDK 全矩阵

> 日期：2026-11-25  
> 结果：Docker Compose 内 14 类 SDK/管理场景全部通过；共 134 条消息。

---

## 1. 端口调整

宿主 9091 被其他容器占用，Compose 将 broker admin 映射为 **9192:9091**；容器内仍 9091。

## 2. 结果

| 场景 | 结果 |
|---|---|
| Compat | 15/15 |
| Push | 5/5 |
| Retry | attempts=2 |
| Rebalance | c1=6 c2=4 |
| Order | 50/50 ordered |
| Delay | early=0 late=1 |
| Tx | commit=1 rollback=0 |
| TxTimeout | visible=0 |
| TxCheckBack | checks=1 visible=1 |
| Idempotent | count=1 |
| DLQ | dlq=1 |
| SQL92 Push | count=1 |
| StartDeliver | early=0 late=1 |
| OffsetProgress | consumed=5 |
| Group CRUD | create=true delete=false |
| Inbox | 3→1 |

## 3. 统计

```text
/stats:
messages=134
topics=23
queues=184
commitlog_bytes=25025
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 5. 限制

- 仅 Docker Compose，未在同一轮跑 K8s 全矩阵；
- 完整 RocketMQ/Prometheus/Grafana Compose 未跑完。
