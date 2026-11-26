# R214 · Raft TCP 长连接/端口复用（负结果，已回退）

> 日期：2026-11-26  
> 结果：为减少 Raft 每次 RPC 新建连接导致的 Windows 端口耗尽，尝试长连接；4 节点启动后无法选出可写 leader，已回退。

---

## 1. 背景

4 节点 leader 日志出现：

```text
connect 127.0.0.1:19532: os error 10048
```

Windows 端口复用/耗尽，源于每个 append/vote 新建 TCP 连接。

## 2. 尝试

- 服务端 `handle` 循环处理同一连接多帧；
- `NetworkClient` 持有 `Option<TcpStream>` 复用连接，失败重连一次。

## 3. 结果

3 节点单元测试通过；4 节点启动后：

```text
所有 broker stats messages=0
heartbeat target=3 Elapsed
producer registered 但 BATCH 无响应
```

无法选出可写 leader，判定为负结果，已回退。

## 4. 结论

- 长连接复用与 openraft 并发/心跳交互存在问题；
- 未继续采用；
- 4 节点重加入与端口耗尽仍未解决；
- 当前仍为每 RPC 新建连接。

## 5. 质量门禁

```text
回退后 cargo fmt / clippy / test 通过（87 项）
```
