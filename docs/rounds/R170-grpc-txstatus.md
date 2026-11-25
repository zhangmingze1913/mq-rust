# R170 · gRPC TxStatus 验证

> 日期：2026-11-26  
> 结果：gRPC `Admin/TxStatus` 通过 grpcurl 调用，R161 遗留的 CLI 验证缺口关闭。

---

## 1. 调用

```powershell
'{"tx_id":"missing"}' | grpcurl -plaintext `
  -import-path crates/mq-broker/proto -proto admin.proto `
  -d '@' 127.0.0.1:9291 mqrust.admin.v1.Admin/TxStatus
```

输出：

```json
{
  "txId": "missing"
}
```

`pending=false` 为 proto3 默认值，JSON 省略。

## 2. 说明

- `pending=true` 需存在 half 消息；本轮只验证不存在路径；
- gRPC Health/Stats 已在 R161 验证；
- gRPC TLS/mTLS 已在 R162 验证。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
