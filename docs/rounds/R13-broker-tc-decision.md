# R13 · Broker 按 TC 决策收口 Half 消息

> 日期：2026-11-25  
> 状态：**Broker 周期查询 TC 状态，对 pending Half 执行 commit/rollback**；单测与 RocketMQ SDK 端到端验证通过。

---

## 1. 机制

- Broker 新增 `--tc-reconcile-ms`（默认 2000）；
- 后台任务遍历 `pending_half()`；
- `GET /tx/{id}` 查询 TC：
  - `committed` → `commit_half` 并 append 到队列；
  - `rolled_back` → `rollback_half`；
  - `prepared`/未知 → 保留。
- 审计事件：`tx.reconcile_commit`、`tx.reconcile_rollback`。

## 2. 单测

```text
test server::tests::reconcile_commits_tc_decided_half ... ok
```

- 构造本地 Half；
- mock TC 返回 `{"status":"committed"}`；
- `reconcile_once` 返回 1，pending Half 清空。

## 2.1 端到端验证

```text
TX TIMEOUT msgId=7F...B70000
TX TIMEOUT RESULT visible=0        # Half 不可见

POST /tx/{id}/commit -> committed  # 直接改 TC 决策

（等待 --tc-reconcile-ms=500）
RECV 5:0 timeout-body
PULL_ONLY received=1 expected=1
```

- Broker 以本地 Half 为 pending；
- TC 被外部提交后，Broker 对账任务将 Half 提交为可见消息；
- 消费端拉取到 1 条。

## 3. 配置

```text
mq-broker ... --tc-url http://127.0.0.1:19401,http://... \
  --tc-reconcile-ms 2000
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       54 项通过
```

## 5. 限制

| 限制 | 计划 |
|---|---|
| 仅处理 pending Half，未做全量对账 | R13.1 ledger |
| 轮询 2s，实时性一般 | 事件驱动/订阅 |
| TC 不可达时保留 Half | 超时策略 |
| 无消费 Inbox | 后续 |
