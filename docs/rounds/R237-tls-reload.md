# R237 · TLS 证书热轮换

> 日期：2026-11-27  
> 结果：新增 `--tls-reload-ms`；替换 cert/key 后新连接使用新证书，无需重启；TLS 握手测试通过。

---

## 1. 变更

- `BrokerConfig.tls_reload_ms`，CLI `--tls-reload-ms`；
- 启动时安装 rustls CryptoProvider（修复 TLS server panic）；
- acceptor 放入 `RwLock`，后台线程按间隔重建；
- 失败只告警，保留旧证书；
- 新连接使用新 acceptor。

## 2. 验证

```text
cert1 serial 1001 -> 替换为 cert2
2s 后新 TLS 连接 serial 2002
broker log: tls certificate reloaded
```

脚本：`H:\mq\tls-check.py`（cryptography + Python ssl）。

## 3. 用法

```text
mq-broker --tls-cert cert.pem --tls-key key.pem --tls-reload-ms 1000
```

## 4. 边界

- 仅 remoting TLS；gRPC TLS 仍为启动加载；
- 轮换期间新连接可能短暂失败；
- 未做 mTLS 客户端 CA 轮换。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（91 项）
TLS 热轮换实测通过
```
