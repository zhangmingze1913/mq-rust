# R04 · release 性能验收（1 KiB / 4 线程 / 10k）

> 日期：2026-11-24  
> 环境：Windows 10、i5-4210M、JDK 17、RocketMQ 4.9.8 客户端、localhost  
> 状态：**release 基线 2,270 TPS**；距离目标 5 万+ TPS 仍远，已定位主要瓶颈。

---

## 1. 结果

| 版本 / 配置 | TPS | Avg RT | Max RT | Failed |
|---|---:|---:|---:|---:|
| debug / JSON 持久化（R1） | 277 | — | — | 0 |
| debug / 二进制持久化（R2） | 1,261 | — | — | 0 |
| release / 同步 fsync/64 | 1,580 | 2.69 ms | 502 ms | 0 |
| release / 后台 flush 10ms | 1,594 | 2.51 ms | 734 ms | 0 |
| release / 后台 flush 1s | **2,270** | **1.90 ms** | **434 ms** | **0** |
| release / dedup 引用优化 / 4 线程 | **3,139** | **1.27 ms** | **355 ms** | **0** |
| **release / dedup 引用优化 / 8 线程** | **4,050** | **1.90 ms** | **338 ms** | **0** |
| release / 8 分片 CommitLog / 8 线程 | 2,265 | 3.73 ms | 595 ms | 0 |
| release / 1 分片 CommitLog / 8 线程 | 3,518 | 2.42 ms | 412 ms | 0 |
| release / 单 writer group commit / 8 线程 | 2,681 | 3.14 ms | 458 ms | 0 |
| release / 默认路径复测 / 8 线程 | 2,410 | 3.34 ms | 377 ms | 0 |
| release / 默认路径复测 / 4 线程 | 2,820 | 1.59 ms | 451 ms | 0 |
| **release / per-topic 锁 / 8 线程** | **4,800** | **1.67 ms** | **591 ms** | **0** |
| **release / per-topic 锁 / 4 线程** | **4,393** | **0.91 ms** | **353 ms** | **0** |
| **release / dedup 16 分片 / 8 线程** | **7,127** | **1.12 ms** | **285 ms** | **0** |
| **release / dedup 16 分片 / 4 线程** | **5,556** | **0.71 ms** | **279 ms** | **0** |

命令：

```powershell
java -cp "$out;$cp" org.apache.rocketmq.example.benchmark.Producer `
  -n 127.0.0.1:9877 -t MQ_RUST_PERF_RELEASE4 -s 1024 -q 10000 -w 4
```

结果：

```text
[Complete] Send Total: 20000 Send TPS: 4050 Max RT(ms): 338 Average RT(ms): 1.902
Send Failed: 0 Response Failed: 0
```

4 线程同配置：

```text
[Complete] Send Total: 10000 Send TPS: 3139 Max RT(ms): 355 Average RT(ms): 1.272
Send Failed: 0 Response Failed: 0
```

---

## 2. 本轮优化

1. **后台刷盘**  
   `append` 不再在请求线程里 fsync；新增 `BrokerStore::sync()`，由 `--flush-interval-ms` 控制后台刷盘。

2. **默认值调整**  
   `--flush-interval-ms` 默认 1000ms，兼顾吞吐与可接受的持久化窗口；金融强一致场景可设 0/1 或改为同步刷盘。

3. **请求级并发实验**  
   尝试每请求 `tokio::spawn`，但锁竞争导致 TPS 降至 1,235，已回退为顺序处理。

4. **事务回归**  
   release 版本仍通过：

   ```text
   TX RESULT commit=1 rollback=0
   {"event":"tx.prepare",...}
   {"event":"tx.commit",...}
   {"event":"tx.rollback",...}
   ```

5. **dedup 引用优化**  
   `dedup` 不再保存整条 `StoredMessage`，只保存 `(queue_id, queue_offset)`；重复发送时按引用回查，减少每消息一次 1 KiB clone。4 线程从 2,270 提升到 3,139 TPS。

6. **分片 CommitLog 实验（负结果）**  
   按 `queue_id % shards` 写 8 个 CommitLog：2,265 TPS，低于单分片 3,518。  
   原因：机械盘多文件随机写导致寻道放大；分片对 NVMe 才有意义。默认 `--commitlog-shards 1`。

7. **单 writer group commit 实验（负结果）**  
   `--group-commit` 用一个 writer 线程批量 `append_batch`：2,681 TPS，默认路径 2,410-2,820（同机波动）。  
   请求级 channel 往返与全局 `topics` 锁抵消了批量写收益，默认关闭。

8. **per-topic 锁（正结果）**  
   `topics` 改为 `HashMap<String, Arc<RwLock<TopicState>>>`，按 topic 独立加锁：  
   8 线程 4,800 TPS、4 线程 4,393 TPS。

9. **dedup 16 分片（正结果）**  
   幂等去重表按 `(topic,msgId)` 哈希拆 16 个 `Mutex`，消除全局写锁：  
   **8 线程 7,127 TPS、4 线程 5,556 TPS**，成为当前最佳基线。

---

## 3. 瓶颈分析

| 瓶颈 | 说明 | 下一步 |
|---|---|---|
| 单连接顺序处理 | 一个 Producer 连接的请求在 Broker 内串行 | 按 queue/连接分片、请求 pipeline |
| `Mutex<CommitLog>` | 所有 append 串行写一个文件 | 分区 CommitLog / 批量 group commit |
| fsync 争用 | 后台 sync 持锁，HDD 延迟高 | 独立 flush thread + 双缓冲 |
| 消息编码分配 | 每条消息 encode 一次并 clone | 零拷贝/复用 buffer |
| 单机 HDD | 4 核移动 CPU + 机械盘 | NVMe/容器环境复测 |
| benchmark 同步发送 | 4 线程共享 Producer 连接 | 多 Producer / 异步发送对比 |

---

## 4. 结论

- release 相比 debug 二进制路径提升约 **5.7 倍**（1,261 → 7,127 TPS）。
- 以当前“单机 + HDD + 单 CommitLog + 单连接顺序处理”架构，**7,127 TPS** 是当前最佳基线，仍**不满足 5 万+ TPS 目标**，但已接近 8k 阶段目标。
- 分片 CommitLog 与单 writer group commit 为负结果；per-topic 锁、dedup 分片为正结果。下一步做**多连接 pipeline + NVMe**。

---

## 5. 复现步骤

```powershell
cargo build --release -p mq-broker
.\target\release\mq-broker.exe `
  --listen 127.0.0.1:10921 `
  --namesrv 127.0.0.1:9877 `
  --queues 8 `
  --store-root G:/MQ_soft/mq-rust-data/r4-release-perf4 `
  --metrics-listen 127.0.0.1:9090 `
  --admin-listen 127.0.0.1:9091 `
  --flush-interval-ms 1000
```

然后运行上面的 `benchmark.Producer`。
