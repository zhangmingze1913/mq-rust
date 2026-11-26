# R209 · gRPC Admin 客户端示例

> 日期：2026-11-26  
> 结果：新增可运行示例 `crates/mq-broker/examples/grpc_admin.rs`，验证 Health/Stats/TxStatus。

---

## 1. 用法

```text
cargo run -p mq-broker --example grpc_admin -- 127.0.0.1:50051 [tx_id]
```

broker 需启用：

```text
--grpc-listen 127.0.0.1:50051
```

## 2. 实测

```text
health status=ok version=0.9.0-rc.1
stats send_total=0 message_bytes=0 commitlog_bytes=0 pending_half=0 topics=0
tx_status tx_id=demo-tx pending=false
```

## 3. 说明

- 使用 tonic `Grpc` 客户端与 `ProstCodec`；
- 每次 unary 前调用 `ready()`；
- 示例不包含 TLS/mTLS；TLS 用法见 docs/17 或 gRPC TLS 轮次。

## 4. 质量门禁

```text
cargo build -p mq-broker --example grpc_admin  通过
cargo fmt / clippy / test                     通过（86 项）
```
