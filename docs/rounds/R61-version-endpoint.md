# R61 · `/version` 服务版本

> 日期：2026-11-25  
> 结果：Broker admin 新增 `GET /version`。

---

## 1. 验证

```text
GET http://127.0.0.1:9091/version
{"service":"mq-broker","version":"0.1.0"}
```

## 2. 说明

- 版本取自 `CARGO_PKG_VERSION`；
- 配置 `--admin-token` 时同样要求 Bearer；
- `/fence` 仍使用独立 fence token。

## 3. 限制

- 无 git commit/构建时间；
- Namesrv/TC 未加 `/version`。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```
