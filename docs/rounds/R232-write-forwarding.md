# R232 · follower 写转发

> 日期：2026-11-27  
> 结果：任意 broker 收到写请求时自动转发到 leader；namesrv 指向 follower 也能 3 副本写入成功。

---

## 1. 实现

- 删除 broker server 层的 `if !raft.is_leader()` 拒绝；
- `MessageRaft::client_write_or_forward`：
  - 先本地 `client_write`；
  - 命中 `ForwardToLeader` 时通过 Raft RPC `Forward(ClientRequest)` 转发；
  - leader 端 `raft.client_write` 并返回结果。
- 修复 K8s 重启后 namesrv 静态路由指向非 leader 的 `not raft leader`。

## 2. 本机验证

```text
3 节点 release
namesrv 指向 broker1（11001）
mqadmin sendMessage -> FWD2
broker1/2/3 messages 全为 1
```

## 3. 意义

- 客户端无需知道 leader；
- namesrv 路由过期不再导致写失败；
- K8s 长跑重启后可直接继续写。

## 4. 边界

- 转发是同步 RPC，leader 不可达时仍失败；
- 批量/延时/事务路径都走同一转发；
- 未做转发重试。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
