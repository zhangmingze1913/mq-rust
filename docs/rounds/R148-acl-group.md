# R148 · ACL 组权限

> 日期：2026-11-25  
> 结果：ACL 权限新增 `group` 维度；PULL 同时校验 topic 与 consumerGroup。

---

## 1. Schema

```json
{"accounts":[{"accessKey":"key","secretKey":"secret",
  "permissions":[{"topic":"MQ_*","group":"G_*","perm":"READ"}]}]}
```

- `group` 缺省 `*`，兼容旧 ACL 文件；
- DENY 同时匹配 topic 与 group 才生效；
- `allows_group(accessKey, topic, group, action, peer)`。

## 2. 服务端

- PULL_MESSAGE 在 topic 校验后，读 `consumerGroup`/`group`；
- 不匹配返回 `NO_PERMISSION`。

## 3. 测试

```text
allows_group(key, MQ_ALLOW, G_READ, WRITE) = true
allows_group(key, MQ_ALLOW, OTHER, WRITE)  = false
allows_group(key, MQ_ALLOW, G_READ, READ)  = false
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
