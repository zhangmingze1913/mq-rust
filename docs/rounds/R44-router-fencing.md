# R44 · Namesrv 路由栅栏（防旧主写入）

> 日期：2026-11-25  
> 结果：Broker 轮询 Namesrv 路由，路由不是自身时进入只读，拒绝 SEND/事务结束。

---

## 1. 机制

- `--fence-url 127.0.0.1:9880`：Namesrv admin 地址；
- `--advertise-addr 127.0.0.1:10921`：自身地址；
- 每 `--fence-interval-ms`（默认 5000）GET `/`，解析 `broker_addr`；
- 路由 ≠ 自身 → `read_only=true`；
- 只读时拒绝 `SEND_MESSAGE`/`SEND_V2`/`BATCH`/`END_TRANSACTION`，返回 `NO_PERMISSION`；
- 路由恢复 → 自动解除。

## 2. 验证

```text
1. 路由 = 127.0.0.1:10921，发送 15/15 成功
2. POST /broker/127.0.0.1:9 改路由
3. 5s 后日志：fence state changed route=127.0.0.1:9 fenced=true
4. 经第二个 Namesrv（仍指向 10921）发送 → NO_PERMISSION，客户端异常重试
5. 路由恢复 10921，发送 15/15 成功
```

## 3. 价值

- 旧主重启后若 Namesrv 已切到新主，旧主自动只读，降低双写/脑裂风险；
- 不需要客户端改动。

## 4. 限制

- 仍非共识/租约，Namesrv 路由本身是单点；
- 只读只覆盖写请求，管理/复制接口未全部栅栏；
- 轮询窗口内仍可能短暂写入（默认 5s）；
- 不能替代严格 Raft 选主。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 6. 涉及文件

- `crates/mq-broker/src/server.rs`：`read_only` 拒绝写
- `crates/mq-broker/src/main.rs`：`--fence-url`/`--advertise-addr`
