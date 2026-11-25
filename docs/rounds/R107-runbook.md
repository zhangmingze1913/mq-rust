# R107 · 运维手册

> 日期：2026-11-25  
> 结果：新增 [docs/17-运维手册.md](../17-运维手册.md)，覆盖端口、启动、配置、存储、观测、回收、HA、ACL、排障与限制。

---

## 1. 内容

- 端口/启动命令；
- 关键参数表；
- 存储布局与不可删文件；
- `/health` `/stats` `/metrics` `/trace`；
- Retention 与段回收；
- quorum + 自动提升 + fence；
- ACL JSON；
- 常见故障；
- 0.9.0-rc.1 限制。

## 2. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 3. 说明

手册以原生 Windows 部署为准；容器/K8s 模板仍保留在 `deploy/`，但未实测。
