# R70 · ACL 权限文件（Topic 读/写）

> 日期：2026-11-25  
> 结果：Broker 支持 `--acl-file`，按账号/topic/读写权限授权；允许 topic 发送成功，未授权 topic 拒绝。

---

## 1. 文件格式

```json
{
  "accounts": [
    {
      "accessKey": "mykey",
      "secretKey": "secret",
      "permissions": [
        {"topic": "MQ_ACL_ALLOW", "perm": "WRITE"}
      ]
    }
  ]
}
```

- `topic` 支持 `*` 与 `prefix*`；
- `perm`: `READ` / `WRITE` / `ANY` / `DENY`；
- `DENY` 优先；
- 无匹配权限 = 拒绝。

## 2. 验证

允许 topic：

```text
ACL RESULT status=SEND_OK msgId=7F0000014158070DEA4E7D81D1640000
```

未授权 topic：

```text
发送异常（NO_PERMISSION）
```

## 3. 实现

| 项 | 说明 |
|---|---|
| `--acl-file` | JSON 策略 |
| 签名 | R69 的 AccessKey/Signature |
| 授权 | SEND/BATCH → WRITE；PULL → READ |
| topic key | V2/BATCH 读 `b`，V1/PULL 读 `topic` |
| 优先级 | ACL file 覆盖 `--acl-secrets` / `--acl-access-keys` |

## 4. 限制

- 仅 topic 维度，无 group/IP；
- 静态文件，无热更新；
- 无 TLS，签名可重放；
- 未校验 `SecurityToken`。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       69 项通过
```

## 6. 涉及文件

- `crates/mq-broker/src/acl.rs`（新增）
- `crates/mq-broker/src/server.rs`
- `crates/mq-broker/src/main.rs`
