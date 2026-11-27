# R221 · 三副本 900s/1M 尝试（ENOSPC 中断）

> 日期：2026-11-27  
> 结果：900s 三副本长跑进行中主机磁盘写满，客户端 4,840 次失败，仅 118k 条落盘；结果无效。

---

## 1. 配置

```text
3 副本 Raft，batch256 / 256B / 4 threads
计划 900s
retain 2,000,000
```

## 2. 现象

```text
A: 118,324
B: 107,045
C: 无响应
Send Failed: 4,840
host: ENOSPC: no space left on device
```

- 任务在写日志/分发包时触发 ENOSPC；
- broker 写入失败，A/B 不一致；
- 该轮不能作为 1M 证据。

## 3. 处理

- 停止全部 broker/JVM；
- 清理历史测试 store（G: 33.8GB → 53.6GB 可用）；
- 结果记录为无效，不改代码。

## 4. 结论

- 3 副本 1M 仍未完成；
- 900s 长跑需要 ≥100GB 空闲 + NVMe；
- 当前主机磁盘/IO 是明确阻塞。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（88 项）
```
