# R160 · Java mTLS 收发通过

> 日期：2026-11-26  
> 结果：Java SDK mTLS 发送与消费均通过；R158 的 Java mTLS 限制已关闭。

---

## 1. 根因

RocketMQ `TlsHelper.buildSslContext(true)`：

```java
if (tlsTestModeEnable) {
    return SslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .build();
}
```

默认 `tls.test.mode.enable=true` 时**不加载客户端证书**，所以 R158 出现 `peer sent no certificates`。

必须显式关闭：

```text
-Dtls.test.mode.enable=false
```

## 2. 客户端配置

```text
-Dtls.enable=true
-Dtls.test.mode.enable=false
-Dtls.client.authServer=true
-Dtls.client.trustCertPath=.../ca.crt
-Dtls.client.certPath=.../client.crt
-Dtls.client.keyPath=.../client.key
```

## 3. 验证

```text
TLS RESULT status=SEND_OK msgId=7F...000000
TLS CONSUME count=1
/stats messages=1
```

broker 日志：

```text
producer registered group="mq_rust_tls_producer"
producer registered group="CLIENT_INNER_PRODUCER"
```

## 4. 服务端

```text
mq-namesrv --tls-cert ... --tls-key ... --tls-client-ca .../ca.crt
mq-broker  --tls-cert ... --tls-key ... --tls-client-ca .../ca.crt
```

无客户端证书连接被拒：`peer sent no certificates`。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
