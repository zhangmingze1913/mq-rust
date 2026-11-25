# R05 · 定时/延时消息（首个可用版本）

> 日期：2026-11-24  
> 状态：**18 级延时 + `delay/` WAL + `__STARTDELIVERTIME` 已打通**。时间轮/HA 待补。

---

## 1. 结果

```text
DELAY EARLY count=0
DELAY RECV delay-10s
DELAY LATE count=1
```

测试：`msg.setDelayTimeLevel(3)`（10s），立即 Pull 不可见，11s 后可见（避免慢宿主上 early pull 越过到期点）。

---

## 2. 实现

1. 发送时识别属性 **`DELAY`**（RocketMQ `MessageConst.PROPERTY_DELAY_TIME_LEVEL` 的值）。
2. `delay_level_ms` 映射 1..18 级：
   `1s/5s/10s/30s/1m/2m/.../2h`。
3. `schedule_delay` 存入 Broker 内存 `delayed` 表。
4. 后台每 200ms 执行 `due_delayed`，到期后走正常 `append`，消费者即可见。
5. `DELAY` 消息不立即写 CommitLog；到期才获得 queue offset。
6. `delay/` WAL 记录 `scheduled/delivered` 事件；重启重放，未到期延时消息继续等待。

---

## 2.1 `__STARTDELIVERTIME`

发送时优先读取 `__STARTDELIVERTIME`（毫秒时间戳）：

- 大于当前时间：按 `t - now` 调度；
- 否则回退 `DELAY` 固定级别。

单测覆盖调度表；端到端：

```text
START SENT at=... due=...
START EARLY at=... count=0
START RECV start-3s
START RESULT early=0 late=1
```

窗口必须大于客户端启动/路由耗时（约 10s），否则 early 会误判。

---

## 3. 已知限制

| 限制 | 计划 |
|---|---|
| 无时间轮/分层调度 | R5.2 时间轮 |
| 延时消息不参与事务 | 保持 RocketMQ 语义 |
| 无 HA 复制 | R6 |

---

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       45 项通过
```

---

## 5. 下一轮 R5.1

1. `delay/` WAL 持久化与重启恢复。
2. ~~`__STARTDELIVERTIME` 任意时间戳~~ ✅。
3. 时间轮调度。
4. 延时消息指标。
