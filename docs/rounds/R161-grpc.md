# R161 · gRPC 管理服务

> 日期：2026-11-26  
> 结果：broker 新增 gRPC `Admin` 服务（Health/Stats/TxStatus），Health 与 Stats 实测通过。

---

## 1. 启动

```powershell
mq-broker.exe ... --grpc-listen 127.0.0.1:9291
```

## 2. Proto

`crates/mq-broker/proto/admin.proto`：

```proto
service Admin {
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);
  rpc TxStatus(TxStatusRequest) returns (TxStatusResponse);
}
```

生成代码已提交 `src/grpc_generated.rs`，**构建不再依赖 protoc**。

## 3. 验证

```text
grpcurl ... Admin/Health
{"status":"ok","version":"0.9.0-rc.1"}

mqadmin sendMessage -> SEND_OK

Admin/Stats
{"sendTotal":"1","messageBytes":"2","commitlogBytes":"164","topics":1}
```

TxStatus 方法已实现（按 half 队列判断 pending），本轮 grpcurl 参数转义未成功，未取得 CLI 输出。

## 4. 依赖

- tonic 0.12 / prost 0.13；
- 服务端仅开启 server，client 未生成；
- 无 TLS gRPC（明文 HTTP/2）。

## 5. 限制

- TxStatus 未做 CLI 验证；
- 未做 gRPC TLS/mTLS；
- 未生成 Rust/Java gRPC 客户端示例；
- 未做压测。

## 6. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
