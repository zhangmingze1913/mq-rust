# R250 · 4 节点 4 broker 性能复测

> 日期：2026-11-27  
> 结果：4 节点各 1 broker，batch256/4 线程 60s，MPS 35–170，15 次失败；跨节点 Raft 开销明显，未达 5 万。

---

## 1. 环境

```text
kind 4 节点
broker 4 × 1，一节点一 broker
namesrv 1
batch256 / 256B / 4 threads / 60s
```

## 2. 结果

```text
MPS 35–170
Send Failed: 15
Max RT 2846ms
```

对比：

```text
4 broker 同节点（R241/R233）：峰值 2.7k
4 节点分散：MPS 显著下降
```

## 3. 判断

- 跨节点网络与 4 核/10GB 资源限制；
- 单 leader 仍承担全部写入；
- 5 万仍不可达。

## 4. 已完成的性能优化

- applied 水位节流；
- 避免重复 encode；
- persistent Raft RPC；
- 4 节点前 60s 0 失败（R229）。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
K8s 4 节点 4 broker 15/15 功能通过
```
