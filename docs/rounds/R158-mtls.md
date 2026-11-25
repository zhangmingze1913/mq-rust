# R158 · mTLS 服务端

> 日期：2026-11-25  
> 结果：broker/namesrv 支持 `--tls-client-ca`，客户端证书校验生效；Java 客户端 mTLS 尚未跑通。

---

## 1. 实现

```powershell
mq-namesrv.exe ... --tls-client-ca G:/MQ_soft/mq-mtls/ca.crt
mq-broker.exe  ... --tls-client-ca G:/MQ_soft/mq-mtls/ca.crt
```

- rustls `WebPkiClientVerifier` + `RootCertStore`；
- 未配置 CA 时保持单向 TLS；
- 配置 CA 后要求客户端证书。

## 2. 证书

```text
CA:     ca.crt / ca.key
Server: server.crt / server.key（SAN localhost/127.0.0.1）
Client: client.crt / client.key（EKU clientAuth）
```

## 3. 验证

Python TLS 1.3：

```text
MTLS OK TLSv1.3 TLS_AES_256_GCM_SHA384
```

无客户端证书：

```text
连接被关闭
server log: peer sent no certificates
```

## 4. Java 客户端

使用：

```text
-Dtls.enable=true
-Dtls.client.authServer=true
-Dtls.client.trustCertPath=.../ca.crt
-Dtls.client.certPath=.../client.crt
-Dtls.client.keyPath=.../client.key
```

broker/namesrv 仍报 `peer sent no certificates`，**Java 客户端 mTLS 未跑通**。单向 TLS（R157）不受影响。

> 后续：R160 已修复 Java 客户端 mTLS（需 `tls.test.mode.enable=false`）。

## 5. 限制

- mTLS 服务端已验证，Java mTLS 待排查（证书链/KeyManager 配置）；
- 未做证书热加载/轮换；
- 管理 HTTP 仍明文。

## 6. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
