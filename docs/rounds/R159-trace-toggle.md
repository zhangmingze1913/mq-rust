# R159 · 轨迹开关与压缩优化

> 日期：2026-11-26  
> 结果：新增 `--trace-enabled`，可在压测/生产关闭轨迹记录；C: 空间清理后环境仍不稳定，未取得可信 TPS。

---

## 1. 变更

```powershell
mq-broker.exe ... --trace-enabled=false
```

- `TraceLog::disabled()`：record 直接返回；
- 默认 true，保持兼容；
- 参数使用 `clap ArgAction::Set`，支持 `--trace-enabled=false`。

## 2. 环境清理

```text
docker image prune
docker builder prune
C: 剩余 0.1GB -> 3.5GB
```

## 3. 性能观测

清理后重跑 batch256 时，JVM/服务启动与连接时序异常，采样窗口内 `/stats` 无增量，**未取得可信 TPS**；此前最佳仍为 37,095 msg/s（R151）。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 5. 限制

- 关闭轨迹会失去 `/trace` 与 JSONL；
- 当前宿主 C: 空间紧张，性能数据波动大；
- 未证明关闭轨迹能提升 batch256。
