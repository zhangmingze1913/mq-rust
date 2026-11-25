# R47 · 栅栏通知鉴权与 Body 修复

> 日期：2026-11-25  
> 结果：`/fence` 支持 Bearer token；修复 admin 只读首包导致 body 丢失的问题。

---

## 1. 鉴权

- Namesrv `--fence-token secret123`；
- Broker `--fence-token secret123`；
- Namesrv 通知时带 `Authorization: Bearer secret123`；
- Broker `/fence` 校验，失败返回 403。

## 2. Body 修复

原 admin handler 只读一次 `read`，当 HTTP header 与 body 分两个 TCP 包时 body 丢失。  
现按 `Content-Length` 循环读取，直到 body 完整。

## 3. 验证

```text
POST /fence 无 token        → 403
POST /fence 带 token        → {"fenced":false,"route":"127.0.0.1:10921"}
POST /broker/127.0.0.1:9    → 通知 route=9 fenced=true
POST /broker/127.0.0.1:10921→ 发送 15/15 成功
```

## 4. 限制

- Namesrv admin `POST /broker/...` 本身仍未鉴权；
- token 明文传输，无 TLS；
- 通知失败无重试，依赖轮询兜底；
- 不能替代严格选主。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 6. 涉及文件

- `crates/mq-broker/src/admin.rs`：token 校验 + body 读取
- `crates/mq-namesrv/src/server.rs`：通知 Authorization
- `crates/mq-broker/src/main.rs` / `crates/mq-namesrv/src/main.rs`：`--fence-token`
