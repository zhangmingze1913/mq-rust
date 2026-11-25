# R172 · K8s 集群内 SDK 回归

> 日期：2026-11-26  
> 结果：测试客户端镜像在集群内运行，默认收发、批量、事务全部通过。

---

## 1. 方法

构建 `mq-rust-testclient`：

```dockerfile
FROM eclipse-temurin:17-jre
COPY .k8s-out /app/out
COPY .k8s-libs /app/lib
ENTRYPOINT ["java", "-cp", "/app/out:/app/lib/*", "MqRustCompatSmoke"]
```

加载到 kind 双节点集群，Pod 内直接访问：

```text
mq-rust-namesrv:9877
mq-rust-broker:10921
```

避免 port-forward + hosts 的客户端挂起问题。

## 2. 结果

```text
默认收发   RESULT received=15 asyncOk=5 send=true
批量       BATCH RESULT received=5
事务       TX RESULT commit=1 rollback=0
```

## 3. 说明

- 测试 Pod 使用 `imagePullPolicy=IfNotPresent`；
- 镜像仅测试用，520MB，不进入交付包；
- kind 集群测试后删除，准备目录 `.k8s-libs/.k8s-out` 不提交。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
