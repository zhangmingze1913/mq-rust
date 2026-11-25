# R102 · 半事务消息重启恢复

> 日期：2026-11-25  
> 结果：half 消息在 broker kill/重启后仍可提交，提交后拉取 1/1。

---

## 1. 测试

新增 `MqRustTxRestart.java`：

```text
send：TransactionMQProducer，本地状态 UNKNOW，输出 txId
kill broker
重启同 store
commit：POST /tx/{txId}/commit
pull：统计可见消息
```

## 2. 结果

```text
TX_RESTART txId=7F0000014D40070DEA4E7E706FE90000
TX_RESTART POST .../commit -> 200
TX_RESTART RESULT visible=1
```

## 3. 结论

- half 消息写入独立 `half` CommitLog；
- 重启 replay 恢复 pending half；
- 30s 超时窗口内提交成功，消息可见；
- 事务状态未丢。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 仅单 broker；
- 未验证超时后的 rollback 重启；
- 未验证多副本 half 状态迁移。
