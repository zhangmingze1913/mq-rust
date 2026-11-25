# R49 · Broker Admin 鉴权

> 日期：2026-11-25  
> 结果：Broker admin 支持 `--admin-token`，除 `/fence` 外全部要求 Bearer。

---

## 1. 机制

- `--admin-token broker123`；
- `/health`、`/stats`、`/tx`、`/replica`、`/inbox/...` 全部校验；
- 无 token 返回 401；
- `/fence` 仍使用独立 `--fence-token`；
- 默认不配置 token，保持向后兼容。

## 2. 验证

```text
GET /stats 无 token   → 401
GET /stats 带 token   → {"body_bytes":0,...}
发送 15 条
GET /stats 带 token   → {"body_bytes":315,"messages":15,"queues":8,"topics":1}
```

## 3. 限制

- token 明文，无 TLS；
- 单 token、无角色/审计；
- 与 Namesrv admin token 独立配置；
- 不能替代 ACL/TLS。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 5. 涉及文件

- `crates/mq-broker/src/admin.rs`：鉴权
- `crates/mq-broker/src/main.rs`：`--admin-token`
