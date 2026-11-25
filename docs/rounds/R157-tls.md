# R157 · TLS 收发

> 日期：2026-11-25  
> 结果：broker/namesrv 均支持 TLS；Java SDK `setUseTLS(true)` 发送与消费通过。

---

## 1. 服务端

```powershell
mq-namesrv.exe --listen 0.0.0.0:9877 ... `
  --tls-cert G:/MQ_soft/mq-tls/server.crt `
  --tls-key  G:/MQ_soft/mq-tls/server.key

mq-broker.exe --listen 0.0.0.0:10921 ... `
  --tls-cert G:/MQ_soft/mq-tls/server.crt `
  --tls-key  G:/MQ_soft/mq-tls/server.key
```

- rustls 0.23 / tokio-rustls 0.26；
- PEM 证书链 + PKCS#8/PKCS#1 私钥；
- namesrv 与 broker 都必须启用，客户端会 TLS 连接两者。

## 2. Java 客户端

```java
producer.setUseTLS(true);
consumer.setUseTLS(true);
```

```text
-Dtls.enable=true
-Dtls.test.mode.enable=true   # 自签证书测试模式
```

## 3. 验证

```text
TLS RESULT status=SEND_OK msgId=7F...DCD0000
TLS CONSUME count=1
```

## 4. 证书

测试自签证书由 Python `cryptography` 生成：

```text
CN=localhost
SAN=DNS:localhost,IP:127.0.0.1
```

生产应使用受信 CA，并在客户端配置 truststore；关闭 `tls.test.mode.enable`。

## 5. 限制

- 未实现 mTLS（客户端证书）；
- 未实现证书热加载/轮换；
- 未做 TLS 性能压测；
- 管理 HTTP（9091）仍为明文。

## 6. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
