# R162 · gRPC TLS/mTLS

> 日期：2026-11-26  
> 结果：gRPC Admin 支持单向 TLS 与 mTLS；无客户端证书被拒。

---

## 1. 启动

```powershell
mq-broker.exe ... --grpc-listen 127.0.0.1:9291 `
  --grpc-tls-cert G:/MQ_soft/mq-mtls/server.crt `
  --grpc-tls-key  G:/MQ_soft/mq-mtls/server.key `
  --grpc-tls-client-ca G:/MQ_soft/mq-mtls/ca.crt
```

- tonic `ServerTlsConfig` + rustls；
- 必须安装默认 CryptoProvider（aws-lc-rs），否则多 provider 冲突 panic。

## 2. 单向 TLS

```text
grpcurl -cacert ca.crt -authority localhost ...
Admin/Health -> {"status":"ok","version":"0.9.0-rc.1"}
Admin/Stats  -> {}
```

## 3. mTLS

```text
grpcurl -cacert ca.crt -cert client.crt -key client.key ...
Admin/Health -> {"status":"ok"}
```

无客户端证书：

```text
Failed to dial: context deadline exceeded
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 5. 限制

- 未生成 gRPC 客户端示例；
- TxStatus 仅实现，未做 CLI 验证；
- 无 gRPC 负载/并发测试。
