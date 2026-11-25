# R149 · /health 免鉴权

> 日期：2026-11-25  
> 结果：配置 `--admin-token` 时 `/health` 仍可免鉴权访问，`/stats` 保持 401。

---

## 1. 变更

```rust
if path != "/fence" && path != "/health" && !admin_authorized { 401 }
```

## 2. 验证

```text
broker --admin-token secret

GET /health（无 token）  -> 200 ok
GET /stats （无 token）  -> 401
GET /stats （Bearer）    -> 200 {messages:0...}
```

## 3. 用途

- K8s readiness/liveness `/health` 在启用 admin token 时仍可用；
- `/fence` 仍保持免鉴权以兼容旧主 fencing（后续可加 fence token）。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
