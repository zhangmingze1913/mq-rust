# R76 · Retention 按 CQ 段删除

> 日期：2026-11-25  
> 结果：当丢弃条数与 CQ 段大小对齐时，直接删除整段；否则回退全量重写。10 条保留 6 条的单测验证通过。

---

## 1. 机制

- `ConsumeQueue::drop_prefix(n)`：
  - 仅允许 `n % segment_entries == 0`；
  - 删除前 n/seg 个段文件，后续段重命名前移；
  - 跨段边界保持完整。
- `BrokerStore::retain`：
  - 每队列丢弃后，若 CQ 长度 == 旧消息数且 drop 对齐段大小 → 调 `drop_prefix`；
  - 否则标记 `needs_repair`，最后统一 `repair_cq`。

## 2. 验证

```text
段大小 4，写 10 条
retain(6) → 丢弃 4（对齐）
read → start=4, len=10, 6 条，offset 4..9
test retention_drops_cq_segments_when_aligned ... ok
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       72 项通过
```

## 4. 限制

- 非对齐 drop 仍全量重写；
- 段大小运行期固定；
- 未按字节/TTL 对齐优化。

## 5. 涉及文件

- `crates/mq-store/src/lib.rs`：`drop_prefix`
- `crates/mq-broker/src/store.rs`：`drop_cq_prefix` / `retain`
