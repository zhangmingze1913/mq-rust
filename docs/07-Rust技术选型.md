# 07 · Rust 技术选型

> 原则：**能自研的核心存储自研，不能自研的用最成熟的库；协议兼容优先于“纯 Rust 洁癖”；Windows 保功能，Linux 保性能。**  
> 版本为 2026-11 设计基线，最终以 `Cargo.lock` 与 CI 验证为准。

---

## 1. 选型原则

1. **协议层不冒险**：RocketMQ Remoting 是 JSON header + 二进制 body，必须用 `serde_json` + `bytes` 精确复刻；不要为了性能改成自定义二进制。
2. **存储层自研**：CommitLog/ConsumeQueue 是核心竞争力，用 `memmap2` + `positioned-io` 自己控制；不把消息塞进 RocksDB。
3. **共识层用成熟库**：Controller/TC 用 `openraft`，不自研 Raft；但消息复制不走 Raft。
4. **运行时统一 tokio**：首发不引入 monoio/glommio；io_uring 作为 Linux 可选 feature。
5. **可观测先用稳定 API**：`tracing` + `metrics` + Prometheus；OTel 后置，避免 beta API 侵入核心。
6. **测试即基础设施**：`criterion`、`loom`、`proptest`、`testcontainers` 从第一天纳入 CI。
7. **依赖最小化**：核心 crate 的依赖树要可审计；禁止引入不维护的库。

---

## 2. 运行时与网络

| 层 | 选型 | 版本基线 | 理由 | 替代/风险 |
|---|---|---|---|---|
| 异步运行时 | **tokio** | 1.4x | 生态最成熟、跨平台、社区大 | monoio/glommio 仅 Linux，生态小；作为后续 feature |
| TCP | `tokio::net` | 随 tokio | 每连接 task + 背压 | 连接数大时需自己做 accept 限流 |
| gRPC | **tonic** + `prost` | 0.14 / 0.14 | 5.x Proxy 标准；与 Java gRPC 兼容 | tonic 大版本 breaking；锁定 minor |
| HTTP | **axum** + `hyper` | 0.8 / 1.x | Admin/健康检查/metrics | 不要用重型框架 |
| TLS | `tokio-rustls` / `rustls` | 0.26 / 0.23 | 无 OpenSSL 依赖、易交叉编译 | 与 Java 客户端 TLS 参数需对齐 |
| 编解码 | `bytes` + `serde_json` | 1.x / 1.x | 零拷贝切片 + JSON header 兼容 | JSON 解析是热路径，需缓存/避免重复解析 |
| 压缩 | `lz4_flex` / `zstd` | 最新稳定 | 消息压缩兼容 RocketMQ（zlib/lz4/zstd） | 先支持 zlib/lz4，zstd 可选 |

> **为什么不用 monoio/glommio 首发**：Broker 是“顺序写 + 海量连接 + 定时任务”混合负载，thread-per-core 在负载倾斜、排空、跨核迁移上更复杂；tokio 的 work-stealing 更适合首版快速覆盖功能。io_uring 通过 feature 逐步引入。

---

## 3. 协议与序列化

| 用途 | 选型 | 说明 |
|---|---|---|
| Remoting header | `serde_json` | 字段名/类型必须与 Java `RemotingCommand` 一致；禁止重命名 |
| Remoting body | `bytes::Bytes` | 避免整包复制；`BytesMut` 复用 |
| gRPC/proto | `prost` + `tonic-build` | 5.x `apache.rocketmq.v2` proto |
| 内部 RPC | `prost` | TC/Broker 内部通信 |
| 内部索引/快照 | 自定义二进制 + CRC32C | 固定布局、可演进；不用 JSON |
| 配置 | `figment` + `serde` | TOML/ENV/CLI 合并 |
| 错误 | `thiserror`（库）+ `anyhow`（进程边界） | 库不泄露 anyhow |
| ID | `uuid` v7 / 自定义雪花 | 消息 ID 需兼容 RocketMQ 格式（ hex） |

> `rkyv`/`flatbuffers` 不作为对外协议：格式演进与 unsafe 成本高；内部快照可评估。

---

## 4. 存储

| 能力 | 选型 | 版本基线 | 说明 |
|---|---|---|---|
| mmap | **memmap2** | 0.9 | 跨平台；CommitLog/ConsumeQueue 映射 |
| 定位读写 | **positioned-io** | 0.3 | `pread/pwrite`，不改变文件游标；多线程安全 |
| CRC | `crc32c`（硬件加速） | 0.6 | 消息体/日志校验 |
| 哈希 | `xxhash-rust` / `ahash` | 最新 | IndexFile/分片 |
| 元数据/索引 KV | **redb** | 3.x | 纯 Rust、单写多读、格式稳定；存 topic/group/offset/dedup |
| 可选 KV | `rocksdb`（feature） | 0.24 | 仅当需要高写入/压缩；C++ 编译重，默认关闭 |
| 内存分配 | `mimalloc`（Linux）/ 系统（Windows） | 最新 | 降低碎片与分配延迟；feature gate |
| 文件锁 | `fs2` | 0.4 | 防止多进程写同一数据目录 |
| 目录遍历 | `walkdir` | 2.x | 启动恢复 |
| 时间 | `std::time::Instant` + `quanta` | 最新 | 单调时钟；墙钟仅审计 |

**不选**：
- `sled`：长期 1.0 alpha，维护风险。
- 直接用 `rocksdb` 存消息体：写放大、 compaction 抖动，不适合顺序日志。
- `tokio-uring` 作为默认：仅 Linux、current-thread runtime 限制。

### 4.1 存储引擎形态

```text
CommitLog: append-only, 1GB segment, mmap + pwrite, group commit
ConsumeQueue: fixed 20B entries, mmap, dispatch async
IndexFile: hash slots + linked list, mmap
Tx WAL: append-only + snapshot, openraft state machine
Delay: timing wheel + delay log
```

**刷盘**：
- `AsyncFlush`：写入 PageCache 即返回；后台按 `flush_interval` 批量 `fsync`。
- `SyncFlush`：组提交，等待 `fsync` 完成；返回 `FLUSH_DISK_TIMEOUT` 若超时。
- Windows：`FlushFileBuffers`；Linux：`fdatasync`/`fsync`。

**零拷贝**：
- Linux：`sendfile`/`splice` 用于大消息拉取；`io_uring` 可选。
- Windows：`TransmitFile`；否则 `write` + `Bytes` 切片。
- 首版先保证正确性，零拷贝作为 R4 优化。

---

## 5. 共识与复制

| 能力 | 选型 | 说明 |
|---|---|---|
| Controller/TC Raft | **openraft** 0.10 | 活跃、API 清晰、支持 snapshot/membership |
| 消息复制 | 自研主从 + ISR | 批量拉取/推送、ack、epoch/fencing |
| 选主 | openraft leader | Controller 管理 Broker 组；TC 管理事务分片 |
| 成员变更 | openraft membership | 3→5 节点扩容 |
| 快照 | openraft snapshot + 本地文件 | TC 状态机快照；Broker 元数据快照 |

**不选 `raft-rs`**：TiKV 的 raft-rs 已长期停在 0.7，维护风险高；openraft 更合适新项目。

**复制语义**：
- `ASYNC_MASTER`：Master 写成功即 ACK；Slave 异步追。
- `SYNC_MASTER`：Master + ISR 多数派写成功才 ACK；可配置 `min_isr`。
- `SYNC_MASTER` + `RPO=0`：至少 1 个 Slave 同步确认；跨 AZ 部署。
- 旧 Master fencing：Controller 发放 epoch，Store 拒绝旧 epoch 写入。

---

## 6. 可观测性

| 能力 | 选型 | 说明 |
|---|---|---|
| 日志 | `tracing` + `tracing-subscriber` | JSON/pretty 双模式；字段化 |
| 指标门面 | `metrics` | 统一 counter/gauge/histogram |
| Prometheus | `metrics-exporter-prometheus` | `/metrics`；避免自研文本格式 |
| Trace | `opentelemetry` + `opentelemetry-otlp` | P1；OTLP gRPC |
| 火焰图 | `pprof` | CPU/heap；按需开启 |
| 审计 | 自研 append-only audit log | 与事务日志同源；WORM 归档 |
| Dashboard | Grafana JSON | 版本化，随仓库发布 |

> 不直接用 `prometheus` crate 的全局注册表：多实例测试困难；用 `metrics` 门面 + exporter。

---

## 7. 测试与工程化

| 能力 | 选型 | 说明 |
|---|---|---|
| 单元/集成 | `cargo test` + `tokio::test` | 默认 |
| 属性测试 | `proptest` | 编解码/状态机/索引 |
| 并发验证 | `loom` | 锁/无锁队列/状态机 |
| 基准 | `criterion` | 吞吐/延迟/分配 |
| 容器 | `testcontainers` | 起 RocketMQ/Java SDK 对打；Docker 不可用时用 native harness |
| 断言 | `insta` | 快照测试（协议 JSON） |
| Mock | `wiremock` | HTTP/gRPC mock |
| 覆盖率 | `cargo-llvm-cov` | CI 报告 |
| 模糊测试 | `cargo-fuzz` | 协议解码/恢复 |
| CLI | `clap` 4.x | `mqctl` |
| 配置 | `figment` | TOML/ENV |
| 错误 | `thiserror`/`anyhow` | 库/进程边界 |
| 日志 | `tracing` | 结构化 |
| 格式化/lint | `rustfmt` + `clippy -D warnings` | CI 门禁 |

---

## 8. 平台差异

| 能力 | Linux | Windows | 策略 |
|---|---|---|---|
| mmap | ✅ | ✅ | `memmap2` |
| `fsync` | `fdatasync` | `FlushFileBuffers` | `platform::flush` |
| 零拷贝 | `sendfile/splice` | `TransmitFile` | `platform::sendfile` |
| io_uring | ✅ | ❌ | feature `io-uring`，默认关 |
| CPU 亲和 | `sched_setaffinity` | `SetThreadAffinityMask` | 可选；不进 SLA |
| 文件删除 | 可删除打开文件 | 不可 | CommitLog 轮转需先 unmap/close |
| 路径/大小写 | 大小写敏感 | 不敏感 | 统一小写 + 规范化 |

**CI**：Linux（性能/集成）+ Windows（编译/功能）双平台；性能数字只在 Linux 认定。

---

## 9. 推荐依赖清单（workspace）

```toml
[workspace.dependencies]
# runtime / net
tokio = { version = "1.4x", features = ["full"] }
tokio-util = "0.7"
futures = "0.3"
bytes = "1"
tonic = "0.14"
prost = "0.14"
axum = "0.8"
hyper = "1"
tower = "0.5"
tokio-rustls = "0.26"
rustls = "0.23"

# serde / config
serde = { version = "1", features = ["derive"] }
serde_json = "1"
figment = { version = "0.10", features = ["toml", "env"] }
clap = { version = "4", features = ["derive"] }
humantime-serde = "1"

# storage
memmap2 = "0.9"
positioned-io = "0.3"
crc32c = "0.6"
xxhash-rust = { version = "0.8", features = ["xxh3"] }
redb = "3"
fs2 = "0.4"
walkdir = "2"
mimalloc = { version = "0.1", optional = true }

# consensus
openraft = "0.10"

# observability
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"
opentelemetry = "0.33"
opentelemetry-otlp = "0.33"
pprof = { version = "0.15", features = ["flamegraph"] }

# errors / utils
thiserror = "2"
anyhow = "1"
uuid = { version = "1", features = ["v7", "serde"] }
parking_lot = "0.12"
crossbeam-channel = "0.5"
dashmap = "6"
smallvec = "1"
ahash = "0.8"

# test
criterion = "0.7"
proptest = "1"
loom = "0.7"
testcontainers = "0.24"
insta = "1"
wiremock = "0.6"
```

> 版本号中的 `1.4x` 等为设计基线，实际以 `cargo add` 解析的最新兼容版本为准；`Cargo.lock` 必须提交。

---

## 10. 选型风险与替代

| 风险 | 影响 | 缓解 | 替代 |
|---|---|---|---|
| `tonic` 0.14 breaking | Proxy 升级成本 | 锁 minor；抽象 `ProxyTransport` | grpc-rust / 自研 HTTP2 |
| `openraft` 0.9→0.10 迁移 | TC 代码改动 | 锁版本；隔离 `mq-txn/raft` | raft-rs（不推荐） |
| `redb` 单写者 | 元数据写入热点 | 分库/分片；缓存 | rocksdb（feature） |
| `memmap2` 在 Windows 删除文件 | CommitLog 轮转失败 | unmap 后删除；引用计数 | 纯 `pread/pwrite`（慢） |
| `metrics` 生态碎片 | exporter 兼容 | 统一门面；集成测试 | `prometheus` crate |
| `testcontainers` 依赖 Docker | CI 环境 | native harness 兜底 | 自研进程管理 |
| `mimalloc` Windows 兼容 | 崩溃/性能 | 仅 Linux feature | 系统分配器 |

---

## 11. 选型结论（10 行）

1. 运行时全栈 **tokio**；io_uring 只做 Linux feature。
2. 协议 **serde_json + bytes**；gRPC **tonic + prost**。
3. 存储自研 **CommitLog + ConsumeQueue + Index**，`memmap2` + `positioned-io`。
4. 元数据/去重 **redb**；不把消息体放进 KV。
5. Controller/TC **openraft**；消息复制主从 + ISR。
6. 可观测 **tracing + metrics + Prometheus**；OTel 后置。
7. 测试 **criterion + proptest + loom + testcontainers**。
8. Windows 保功能、Linux 保性能；平台抽象层。
9. 不引入 sled/raft-rs；rocksdb 仅可选。
10. 所有版本锁定 + `Cargo.lock` + CI 双平台。
