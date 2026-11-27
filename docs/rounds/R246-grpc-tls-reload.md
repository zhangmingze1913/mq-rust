# R246 · gRPC TLS 热轮换

> 日期：2026-11-27  
> 结果：新增 `--grpc-tls-reload-ms`；gRPC TLS 证书/客户端 CA 变更后自动重启 tonic 监听；TLS 握手 serial 1001→2002。

---

## 1. 变更

- `GrpcTls.reload_ms`；
- `load_tls` / `tls_mtime`；
- `serve` 监督循环：
  - 运行 tonic `serve_with_shutdown`；
  - 轮询 cert/key/ca mtime；
  - 加载成功后优雅关闭旧 server，重建并重新绑定；
  - 加载失败保留旧配置。

## 2. 验证

```text
broker gRPC TLS 9443
first serial 1001
替换 cert1 -> cert2
10s 内新连接 serial 2002
log: grpc tls certificate reloaded
```

脚本：`H:\mq\grpc-tls-check.py`。

## 3. 用法

```text
mq-broker --grpc-listen 0.0.0.0:9443 \
  --grpc-tls-cert cert.pem --grpc-tls-key key.pem \
  --grpc-tls-reload-ms 1000
```

## 4. 边界

- 轮换会短暂重启 gRPC 监听（毫秒级）；
- 已有连接可能断开；
- mTLS 客户端 CA 变更同样触发重建。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（92 项）
gRPC TLS serial 1001→2002
```
