# R168 · Inbox 容器场景（15/15）

> 日期：2026-11-26  
> 结果：Inbox 在 R165 镜像上通过；14 类矩阵补齐为 15/15。

---

## 1. 原因

首次 Inbox 失败：

```text
INBOX RESULT delivered=3 processed=0 done=true
```

不是 broker 丢失去重，而是容器卷中 `/data/inbox.log` 已存在上一轮 `business-1` 记录，三次发送都被判为重复。

清空 inbox 文件并重启 broker：

```text
docker exec ... rm -f /data/inbox.log
docker restart ...
```

## 2. 结果

```text
INBOX RESULT delivered=3 processed=1 done=true
```

## 3. 变更

`deploy/docker-compose.lite.yml` 增加 admin 直连映射：

```yaml
- "9192:9091"
- "9091:9091"
```

## 4. 结论

- Inbox 去重跨重启持久化正确；
- 测试需隔离 inbox 状态（每次清空或使用唯一 key）；
- R165 镜像 15/15 场景通过。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
