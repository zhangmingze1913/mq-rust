# R139 · 轨迹异步写入与采样

> 日期：2026-11-25  
> 结果：轨迹改用后台线程 + 有界通道；新增 `--trace-sample-rate`。测试 82 项。

---

## 1. 实现

- `TraceLog` 持有 `SyncSender<Vec<u8>>`，后台线程负责 BufWriter/轮转；
- 发送路径只做 `try_send`，不打开/写文件；
- `--trace-sample-rate N`：每 N 条记录 1 条（内存与 JSONL 同时采样），默认 1；
- 轮转逻辑保留在后台线程。

## 2. 性能观测

```text
100k × 256B / 8 线程：
- 无 trace：1,635 TPS（本次宿主状态）
- trace 全量：1,687 TPS（前一轮）
- trace sample100：4,876 / 927 TPS（波动大）
```

宿主 HDD/CPU 状态波动明显，**无法得出可靠结论**；轨迹默认关闭，建议仅在排障时开启并配合采样。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```

## 4. 限制

- 后台线程崩溃/通道满会丢轨迹；
- 采样后 `/trace/{msgId}` 可能查不到未采样消息；
- 轮转仍只保留一个备份。
