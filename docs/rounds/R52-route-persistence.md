# R52 · Namesrv 路由持久化

> 日期：2026-11-25  
> 结果：`--route-file` 持久化 broker 地址与 epoch，重启不再回到初始路由。

---

## 1. 机制

- Namesrv `--route-file <path>`；
- 启动时读取 `{"broker_addr":"...","epoch":N}`，覆盖初始参数；
- 每次 `POST /broker/{addr}` 后**原子写回**：临时文件 + `sync_all` + rename；
- 文件缺失/损坏时回退 CLI 参数与 epoch=1。

## 2. 验证

```text
启动：--broker-addr 127.0.0.1:10921 --route-file route-r100.json
POST /broker/127.0.0.1:10921 → 文件 {"broker_addr":"127.0.0.1:10921","epoch":2}
停止 Namesrv
重启：--broker-addr 127.0.0.1:9（故意不同）+ 同一 route-file
GET / → {"broker_addr":"127.0.0.1:10921","epoch":2}
临时文件无残留，主文件 43B
```

## 3. 价值

- Namesrv 重启不丢失主路由；
- epoch 连续，避免旧主因 epoch 回退被误解除栅栏；
- 与 R45/R46/R47 栅栏链路配合。

## 4. 限制

- 单文件；临时文件 + rename 已避免半写；
- 仍单 Namesrv，无共识/多副本；
- 文件需放持久卷。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 6. 涉及文件

- `crates/mq-namesrv/src/server.rs`：写 route_file
- `crates/mq-namesrv/src/main.rs`：加载 `--route-file`
