# R231 · K8s 300s 尾段与心跳调优（负结果）

> 日期：2026-11-27  
> 结果：9GB Docker + 1.5Gi broker 可完成 300s；160,512 条、128 次超时、MPS 从 2.7k 降到 483。心跳 500ms 试验更差，已回退。

---

## 1. 300s 结果（9GB VM）

```text
Docker VM 9.37GB / 3 CPU
broker limit 1.5Gi
batch256 / 4 threads / 300s
messages: 160,512
Send Failed: 128
MPS 483–995（尾段）
Max RT 3003ms
0 重启
```

- 可完成，无控制面崩溃；
- 尾段吞吐下降、128 次超时；
- 未达 5 万。

## 2. 心跳 500ms 试验

```text
heartbeat 500 / election 1500–3000
120s: 4,352 失败，MPS 229–568
broker1 成为 leader
```

比 200ms 更差，已回退 200/600–1200。

## 3. 判断

- K8s 单机 3 broker、1 CPU limit 是主要瓶颈；
- 重启后静态 namesrv 路由可能指向非 leader，导致 `not raft leader`；
- 需写转发或 leader 路由。

## 4. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
