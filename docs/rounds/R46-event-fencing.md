# R46 · 事件驱动栅栏通知

> 日期：2026-11-25  
> 结果：Namesrv 路由变更时主动 POST Broker `/fence`，无需等待轮询。

---

## 1. 机制

- Namesrv `--fence-notify 127.0.0.1:9091`（逗号分隔多个 Broker admin）；
- `POST /broker/{addr}` 更新路由后：
  - epoch +1；
  - 向每个 notify 目标 `POST /fence`，body `{"broker_addr":...,"epoch":N}`；
- Broker `/fence` 立即设置 `read_only = route != advertise`；
- 轮询仍保留作为兜底。

## 2. 验证

```text
Broker 轮询间隔故意设为 60s（仅事件通知生效）

1. 发送 15/15 成功
2. POST /broker/127.0.0.1:9 → epoch2
3. 1s 内 Broker 日志：
   WARN mq_broker::admin: broker fence notification route=127.0.0.1:9 fenced=true
4. 经 Namesrv B 发送 → NO_PERMISSION 异常
5. 路由恢复 10921（epoch3）→ 发送 15/15 成功
```

## 3. 价值

- 栅栏窗口从轮询间隔（5s）降到网络往返（毫秒级）；
- 不依赖 Broker 轮询频率；
- 保留 epoch 持久化，重启仍生效。

## 4. 限制

- 通知为 best-effort，不重试/不鉴权；
- Broker admin 需可达；
- Namesrv 单点/epoch 非共识；
- 通知丢失时仍依赖轮询兜底。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 6. 涉及文件

- `crates/mq-namesrv/src/server.rs`：`fence_notify` / `notify_fence`
- `crates/mq-broker/src/admin.rs`：`POST /fence`
- `crates/mq-broker/src/main.rs`：admin 传入 `read_only`/`advertise`
