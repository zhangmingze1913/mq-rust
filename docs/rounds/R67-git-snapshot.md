# R67 · Git 交付快照

> 日期：2026-11-25  
> 结果：仓库初始化并提交两次，工作树干净；运行日志/构建产物已忽略。

---

## 1. 提交

```text
2b2a14f MQ-rust: RocketMQ 4.9.8 compatible broker, namesrv, TC Raft, tests and docs
4956888 chore: ignore runtime logs and build artifacts
```

## 2. 忽略

```text
/target
data/
logs/
*.log
*.log.err
tests/compat/out/
tools/java/out/
```

## 3. 状态

```text
git status --short  空
```

## 4. 说明

- 无远端仓库（本地快照）；
- 保留源码、文档、图表、部署资产、Java 测试源码与脚本；
- 运行日志与 .class 不进入版本库。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```
