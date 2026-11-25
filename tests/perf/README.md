# 性能测试（R1+）

- Rust 压测器：`tools/mq-bench`（规划）
- Java 基线：RocketMQ `org.apache.rocketmq.example.benchmark.Producer/Consumer`
- 指标：TPS、P50/P99/P999、错误率、CPU/RSS、磁盘 IO、副本延迟
- 方法：预热 60s + 测量 300s + 3 次取中位数

详见 `docs/11-性能基准与容量规划.md`。
