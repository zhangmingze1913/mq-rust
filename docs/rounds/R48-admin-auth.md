# R48 · Namesrv Admin 鉴权

> 日期：2026-11-25  
> 结果：Namesrv admin GET/POST 支持 Bearer token；无 token 返回 401。

---

## 1. 机制

- Namesrv `--admin-token secret123`；
- `GET /`、`POST /broker/{addr}` 校验 `Authorization: Bearer ...`；
- 失败 401；
- Broker 轮询 `namesrv_route` 使用 `--fence-token` 作为 Bearer；
- 事件通知同样带 token。

## 2. 验证

```text
GET  /              无 token → 401
GET  /              带 token → {"broker_addr":"127.0.0.1:10921","epoch":1}
POST /broker/9      无 token → 401
POST /broker/9      带 token → epoch2，Broker fenced=true
POST /broker/10921  带 token → epoch3
发送 15/15 成功
```

## 3. 限制

- token 明文，无 TLS；
- 单 token、无角色/审计；
- 无速率限制；
- Namesrv 仍单点。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 5. 涉及文件

- `crates/mq-namesrv/src/server.rs`：admin 鉴权
- `crates/mq-namesrv/src/main.rs`：`--admin-token`
- `crates/mq-broker/src/main.rs`：轮询带 token
