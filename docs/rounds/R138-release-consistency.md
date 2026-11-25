# R138 · 发布一致性检查

> 日期：2026-11-25  
> 结果：新增 `scripts/check-docs.ps1`，校验测试数、SHA256、轮次与链接。

---

## 1. 脚本

`scripts/check-docs.ps1`：

- README/15/16/20/CHANGELOG 必须含 `82 项`；
- 分发包 SHA256 必须与 20/16/21 一致；
- README/20 必须提到当前轮次；
- 扫描 docs/README/CHANGELOG 的相对链接。

## 2. 修复

- `15` 的 R128 行 80 → 82；
- `CHANGELOG` 81 → 82；
- 文档轮次/版本推进到 R138。

## 3. 执行

```text
powershell -File scripts/check-docs.ps1
docs consistency: OK
sha256: <实际 zip 哈希>
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       82 项通过
```
