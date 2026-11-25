# R69 · ACL Signature/HMAC 校验

> 日期：2026-11-25  
> 结果：Broker 支持 `--acl-secrets mykey:secret`，按 RocketMQ 4.9.8 算法校验 `AccessKey` + `Signature`；正确密钥 SEND_OK，错误密钥拒绝。

---

## 1. 实现

- 无外部依赖实现 SHA1 / HMAC-SHA1 / Base64；
- 签名内容：
  - `extFields` 按 key 排序；
  - 拼接所有 **value**（排除 `Signature`）；
  - 追加 body；
  - `Base64(HMAC-SHA1(content, secret))`；
- `--acl-secrets key1:secret1,key2:secret2`；
- 与旧 `--acl-access-keys` 二选一，secrets 优先。

## 2. 验证

正确 secret：

```text
ACL RESULT status=SEND_OK msgId=7F0000014BA0070DEA4E7D79F8100000
```

错误 secret：

```text
发送异常（NO_PERMISSION，客户端重试/失败）
```

## 3. 单测

```text
sha1_known_vectors ... ok
base64_known_vectors ... ok
hmac_known_vector ... ok
```

HMAC 向量与 Python `hmac.new(b"secret", b"hello", sha1)` 一致：
`URIFXAX5RPhXVe/FzYlw4ZTp9Fs=`。

## 4. 限制

- 只有签名校验，无 topic/group 级权限模型；
- 无 TLS，密钥/签名明文传输；
- 无时间戳/nonce，签名可重放；
- 旧 `--acl-access-keys` 仍是不校验签名的不安全模式。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       68 项通过
```

## 6. 涉及文件

- `crates/mq-broker/src/hmac.rs`（新增）
- `crates/mq-broker/src/server.rs`
- `crates/mq-broker/src/main.rs`
