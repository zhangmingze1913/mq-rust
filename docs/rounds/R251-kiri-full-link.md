# R251 · Kiri 容器全链路压测

> 日期：2026-11-28  
> 结果：Kiri 内 3 broker Raft + namesrv + 测试容器；SDK 15/15；60s 全链路 99,840 发/收、0 失败；300s 出现 35,584 失败、p99 14.4s。

---

## 1. 环境

```text
WSL2 发行版 kiri（Alpine 3.24）
kiri daemon + docker API（/var/lib/kiri-daemon/.kiri/run/docker.sock）
3 broker Raft 容器（mq-broker-1/2/3）
namesrv 容器 mq-ns
测试镜像 mq-rust-test:latest（4.9.8 SDK + 新增 MqRustFullLink）
```

## 2. 功能

```text
KIRI_SMOKE: RESULT received=15 asyncOk=5 send=true
```

## 3. 全链路 60s

```text
MqRustFullLink FULL_LINK 4 4 60 256
sent=99,840
failed=0
received=99,840
p50=558ms
p99=1557ms
```

约 1,664 msg/s 端到端，消费者无积压。

## 4. 全链路 300s

```text
FULL_LINK_300 4 4 300 256
sent=177,920
failed=35,584
received=210,529
p50=725ms
p99=14,430ms
```

- 发送超时后消息仍可能落盘，received 大于 sent；
- 300s 尾段明显退化，失败率约 17%。

## 5. 热点判断

- 3 broker 共享 4 CPU；
- Raft 同步复制 + CommitLog 写；
- 生产/消费同 JVM；
- 3s 发送超时在尾段触发大量重试。

## 6. 产物

- `tests/compat/src/MqRustFullLink.java`；
- 测试镜像内含 `MqRustFullLink`；
- Kiri 容器 network `mqnet`。

## 7. 质量门禁

```text
cargo fmt / clippy / test  通过（93 项）
Kiri SDK 15/15
```
