# R117 · 容器清单静态修复

> 日期：2026-11-25  
> 结果：修复 compose 宿主端口冲突与 K8s TC peer ID 不匹配；YAML 解析仍通过。

---

## 1. 修复

| 文件 | 问题 | 修复 |
|---|---|---|
| `deploy/docker-compose.yml` | Prometheus 宿主 9091 与 broker admin 9091 冲突 | 改为 `9092:9090` |
| 同上 | 注释仍写“R1 骨架” | 改为当前说明 |
| `deploy/k8s/mq-rust.yaml` | Pod 后缀 0/1/2，peers 却写 1/2/3 | peers 改为 0/1/2 |

## 2. 静态校验

```text
docker-compose.yml  1 doc
k8s/mq-rust.yaml    7 docs
prometheus.yml      1 doc
```

## 3. 说明

- 仍无 Docker/Podman/K8s 运行时，未构建镜像/未 apply；
- 修复仅消除明显的端口/ID 配置错误；
- 完整验收步骤见 [18-容器化差距](../18-容器化差距.md)。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```
