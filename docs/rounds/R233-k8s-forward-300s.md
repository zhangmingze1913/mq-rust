# R233 · R232 镜像 K8s 300s（写转发）

> 日期：2026-11-27  
> 结果：写转发镜像在 K8s 300s 写入 226,816 条、114 次超时、0 重启；比 R231 的 160k/128 改善。

---

## 1. 环境

```text
Docker VM 9.37GB / 3 CPU
broker limit 1.5Gi，探针 3s/5
kind 3 节点
mq-rust:latest（R232 写转发）
namesrv 静态路由 broker-1
```

## 2. 结果

```text
batch256 / 4 threads / 300s
messages: 226,816
Send Failed: 114
MPS 尾段 28–824，峰值 2.7k
Max RT 3387ms
0 重启
```

对比 R231：160,512 / 128 次超时 / 0 重启。

## 3. 结论

- 写转发后 namesrv 指向 follower 仍可写；
- 长跑不再因 leader 变化产生 `not raft leader`；
- 尾段 CPU/IO 仍导致吞吐下降；
- 5 万不可达。

## 4. 5 万差距

```text
当前峰值 ~2.7k，300s 平均 ~756/s
5 万需要约 20x 提升
单机 4 核 / 3 副本 / 单 leader 无法达到
```

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
K8s 300s 0 重启
```
