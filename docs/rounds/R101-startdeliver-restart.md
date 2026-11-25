# R101 · 定时消息重启恢复

> 日期：2026-11-25  
> 结果：`__STARTDELIVERTIME` 消息在到期前 broker 被 kill/重启，恢复后仍送达 1 条。

---

## 1. 测试

新增 `tests/compat/src/MqRustStartDeliverRestart.java`：

```text
send：__STARTDELIVERTIME = now + 8000
立刻 kill broker
重启同 store
recv：等待 12s
```

## 2. 结果

```text
START_RESTART SENT due=...
START_RESTART RECV restart-delayed
START_RESTART RESULT count=1
```

## 3. 结论

- 延时消息持久化在 `delay` CommitLog；
- 重启 replay 后重新调度；
- 到期仍可消费，无丢失。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 仅单 broker；
- 未验证多副本下延迟状态同步；
- 12s 窗口较窄。
