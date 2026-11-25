# R72 · ACL 来源 IP 白名单

> 日期：2026-11-25  
> 结果：ACL 账号支持 `whiteRemoteAddress`；本机允许、改为 `10.0.0.1` 后拒绝（热更新生效）。

---

## 1. 格式

```json
{
  "accounts": [{
    "accessKey": "mykey",
    "secretKey": "secret",
    "whiteRemoteAddress": "127.0.0.1",
    "permissions": [{"topic": "MQ_ACL_IP", "perm": "WRITE"}]
  }]
}
```

- `*` 任意；
- `prefix*` 前缀；
- 精确 IP。

## 2. 验证

```text
whiteRemoteAddress=127.0.0.1 → ACL RESULT SEND_OK
热更新为 10.0.0.1 → 发送异常（NO_PERMISSION）
```

## 3. 实现

- `handle_connection` 接收 `SocketAddr`；
- `handle_command` 传入 peer；
- `AclPolicy::allows(..., peer_ip)` 先校验 IP 再校验 topic。
- IP 从 TCP 对端获取，不信任客户端字段。

## 4. 限制

- 只支持精确/前缀，无 CIDR/IPv6 段；
- 代理/NAT 后 peer 为代理地址；
- 仍无 TLS。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       69 项通过
```
