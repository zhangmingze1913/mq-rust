# R55 · AccessKey 访问控制（原型）

> 日期：2026-11-25  
> 结果：Broker `--acl-access-keys mykey` 时，无 AccessKey 的客户端被拒绝；带 RocketMQ `AclClientRPCHook` 的客户端发送成功。

---

## 1. 机制

- `--acl-access-keys key1,key2`（逗号分隔）；
- 每个请求检查 extFields `AccessKey`；
- 不在白名单 → `NO_PERMISSION`；
- 不校验 `Signature`（原型，非安全实现）；
- 默认空列表 = 不启用。

## 2. 验证

带 ACL hook：

```text
ACL RESULT status=SEND_OK msgId=7F0000010360070DEA4E7D54B7370000
```

普通客户端：

```text
发送异常（NO_PERMISSION，客户端重试后失败）
```

## 3. 风险

| 风险 | 说明 |
|---|---|
| 只校验 AccessKey | 可伪造/重放，**不是安全 ACL** |
| 无 Signature/HMAC 校验 | 不满足 RocketMQ ACL 语义 |
| 无权限模型 | 无 topic/group 级授权 |
| 明文 key | 无 TLS |

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 5. 涉及文件

- `crates/mq-broker/src/server.rs`：AccessKey 检查
- `crates/mq-broker/src/main.rs`：`--acl-access-keys`
- `tests/compat/src/MqRustAclSmoke.java`
