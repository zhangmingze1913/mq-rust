# R234 · K8s 8 线程上限（负结果）

> 日期：2026-11-27  
> 结果：8 线程 batch256 60s 只有约 22,784 失败、MPS 0–58；并发提高反而压垮单 leader。

---

## 1. 配置

```text
K8s 3 broker Raft（R232 写转发）
batch256 / 256B / 8 threads / 60s
```

## 2. 结果

```text
Send Failed: 89
Send Message Failed: 22,784
MPS 0–58（尾段）
Max RT 2426ms
```

## 3. 对比

```text
4 threads 60s: 206,336，0 失败，峰值 2.7k
8 threads 60s: 失败 22,784，MPS ~0
```

## 4. 结论

- 单 leader 1 CPU limit 已饱和；
- 增加客户端线程只会放大超时；
- 5 万不可达。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
