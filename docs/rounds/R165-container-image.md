# R165 · 容器镜像重建与容器内 gRPC mTLS

> 日期：2026-11-26  
> 结果：镜像重建到 R164；容器内 gRPC mTLS Health 通过。

---

## 1. 镜像

```text
mq-rust:0.9.0-rc.1
image id: sha256:c2f7c0e3bd50a13e91604a1eeb1280465eefe0c57f7ae9f141149b4604619ac2
```

包含：

- TLS/mTLS（remoting）；
- gRPC Admin + gRPC TLS/mTLS；
- Raft 租约与自动路由参数；
- `--trace-enabled`。

## 2. 容器内验证

```powershell
docker run -d --name mq-rust-grpctest `
  -p 10951:10951 -p 9391:9391 `
  -v 'G:\MQ_soft\mq-mtls:/certs:ro' `
  mq-rust:0.9.0-rc.1 `
  --listen 0.0.0.0:10951 --namesrv 127.0.0.1:9877 `
  --grpc-listen 0.0.0.0:9391 `
  --grpc-tls-cert /certs/server.crt `
  --grpc-tls-key /certs/server.key `
  --grpc-tls-client-ca /certs/ca.crt
```

```text
grpcurl -cacert ca.crt -cert client.crt -key client.key ...
Admin/Health
{"status":"ok","version":"0.9.0-rc.1"}
```

## 3. 镜像内参数

```text
--grpc-listen
--grpc-tls-client-ca
--tls-client-ca
--leader-lease
```

## 4. 边界

- 未重跑完整 5 容器 Compose/SDK 矩阵；
- 未跑 K8s；
- 容器内 namesrv 地址在测试中不可达，仅验证 broker 与 gRPC。

## 5. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       83 项通过
```
