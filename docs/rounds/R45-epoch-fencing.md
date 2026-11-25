# R45 · 路由 Epoch 栅栏

> 日期：2026-11-25  
> 结果：Namesrv 路由带单调 epoch；Broker 持久化已见最大 epoch，旧 epoch 的“自己”路由仍被栅栏。

---

## 1. 机制

- Namesrv 启动 `epoch=1`；
- `POST /broker/{addr}` 每次 `epoch+1`；
- `GET /` 返回 `{"broker_addr":...,"epoch":N}`；
- Broker 将已见最大 epoch 写入 `store-root/fence_epoch`；
- 栅栏条件：
  - `route != advertise`，或
  - `route == advertise && epoch < last_epoch`；
- epoch 增大后自动解除栅栏。

## 2. 验证

```text
1. 路由=10921 epoch1 → 发送 15/15
2. POST /broker/127.0.0.1:9 → epoch2，fence_epoch=2
3. 重启 fresh Namesrv（epoch1，路由 10921）
4. Broker 日志：route=10921 epoch=1 last_epoch=2 fenced=true
5. POST /broker/127.0.0.1:10921 → epoch2
6. 发送 15/15 成功
```

## 3. 价值

- 旧主即使被重新指回路由，只要 epoch 落后，仍保持只读；
- 重启后仍生效（epoch 持久化）。

## 4. 限制

- Namesrv 仍是单点，epoch 非 Raft 共识；
- 只读覆盖 SEND/BATCH/END_TRANSACTION；
- 轮询窗口内短暂可写；
- 不能替代严格选主。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 6. 涉及文件

- `crates/mq-namesrv/src/server.rs`：`broker_epoch`
- `crates/mq-broker/src/main.rs`：`fence_epoch` 持久化与比较
