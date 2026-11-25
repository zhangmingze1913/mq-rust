# mq-bench（规划）

Rust 原生压测器，R1 后实现：

```bash
mq-bench send --namesrv 127.0.0.1:9876 --topic BenchTest \
  --size 1024 --threads 16 --count 100000 --mode async

mq-bench consume --namesrv 127.0.0.1:9876 --topic BenchTest \
  --group bench --threads 16 --duration 300
```

输出：TPS、P50/P99/P999、错误率、资源占用；支持导出 JSON/CSV 供报告使用。
