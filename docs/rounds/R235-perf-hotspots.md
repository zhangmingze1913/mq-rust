# R235 · 性能热点分析与优化

> 日期：2026-11-27  
> 结果：定位 3 个热点并优化；4 节点 Raft 60s 144,128 条、四副本一致；persistent RPC 在 4 节点稳定（R214 曾失败）。

---

## 1. 热点

| 热点 | 证据 | 优化 |
|---|---|---|
| applied 水位每条日志写文件 + rename | `persist_applied` 每次 apply 调用 | 500ms 节流；快照/安装仍强制写 |
| append_batch 每条消息 encode 两次 | 一次写 CommitLog，一次算 CQ len | 复用 payload.len()，仅 writer 路径回退 |
| 每个 Raft RPC 新建 TCP 连接 | `TcpStream::connect` 每次 call；服务端一次请求即关 | 客户端持久连接 + 服务端循环读写；失败重连一次 |

## 2. 代码

- `MessageSM::persist_applied(log_id, force)`；
- `BrokerStore::append_batch` 返回 encoded_lens；
- `NetworkClient { conn: Mutex<Option<TcpStream>> }`；
- `handle` 循环处理长度前缀请求。

## 3. 验证

```text
3 节点 release: 1 条消息 3 副本一致
4 节点 release: 1 条消息 4 副本一致
persistent RPC 4 节点正常（R214 曾 no leader）
```

60s batch256/4 线程：

```text
4 节点: 144,128 条，73 次失败，峰值 2.74k
3 节点（优化前/后同机参考）: 123,904 条，78 次失败
```

## 4. 仍存热点

- Raft RPC JSON 序列化；
- 单 leader CPU 上限；
- CommitLog 写盘/组提交；
- CQ/dedup 锁粒度；
- K8s 单机 4 核。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
