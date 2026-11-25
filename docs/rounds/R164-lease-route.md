# R164 · 租约主自动发布 namesrv 路由

> 日期：2026-11-26  
> 结果：broker 获得 Raft 租约后自动把 namesrv 路由切到自己；broker 故障后新主自动接管路由。

---

## 1. 实现

Broker 新增：

```text
--advertise 127.0.0.1:10921
--namesrv-admin 127.0.0.1:9878
```

- 租约线程检测到 `holder == self` 且由非主变为主时：
  - `POST /broker/<advertise>` 到 namesrv admin；
  - namesrv 更新 `broker_addr`、epoch++；
  - 写入 route_file（如配置）。
- 租约续期不重复发布。

## 2. 验证

初始：

```text
broker-a holder
namesrv: {"broker_addr":"127.0.0.1:10921","epoch":2}
broker-a read_only=false
broker-b read_only=true
```

停止 broker-a：

```text
broker-b 在 TTL 后获得租约
namesrv: {"broker_addr":"127.0.0.1:10931","epoch":3}
broker-b read_only=false
broker-b log: published leader route to namesrv advertise=127.0.0.1:10931
```

路由自动切换，无需手工 POST。

## 3. 边界

- 依赖 namesrv admin 可达且未启用 admin token（本轮测试无 token）；
- 路由切换发生在租约过期/新主产生后，存在秒级窗口；
- 消息日志仍是 quorum 复制；
- 未做旧主恢复后的自动回切/防回切测试。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
