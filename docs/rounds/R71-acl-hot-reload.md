# R71 · ACL 策略热更新

> 日期：2026-11-25  
> 结果：`--acl-file` 支持周期重载；实测允许 → 改文件为拒绝 → 发送被拒。

---

## 1. 机制

- `--acl-file` + `--acl-reload-ms`（默认 5000）；
- 后台线程周期读取文件；
- 解析成功才替换当前策略（失败保留旧策略）；
- 请求路径读锁读取策略。

## 2. 验证

```text
初始策略：MQ_ACL_HOT WRITE
ACL RESULT status=SEND_OK

修改为：permissions=[]
等待 3s（reload=1000）
发送 MQ_ACL_HOT → 异常（NO_PERMISSION）
Broker 日志：acl policy reloaded
```

## 3. 限制

- 文件读取失败时不回滚/不告警（仅保留旧策略）；
- 无版本号/原子文件要求；
- 仅 topic 维度；
- 无 TLS。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       69 项通过
```
