# R77 · ACL IP CIDR 支持

> 日期：2026-11-25  
> 结果：`whiteRemoteAddress` 支持 IPv4/IPv6 CIDR（如 `10.0.0.0/8`、`2001:db8::/32`）。

---

## 1. 规则

- `*` 任意；
- `prefix*` 前缀；
- 精确 IP；
- CIDR：IPv4 0–32、IPv6 0–128；
- 非法 CIDR 拒绝。

## 2. 验证

```text
127.0.0.0/8   ↔ 127.0.0.1     允许
10.0.0.0/8    ↔ 10.1.2.3      允许
10.0.0.0/8    ↔ 11.0.0.1      拒绝
2001:db8::/32 ↔ 2001:db8::1   允许
2001:db8::/32 ↔ 2001:db9::1   拒绝
127.0.0.0/33  → 拒绝
```

单测：`ip_cidr_matching ... ok`。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       73 项通过
```

## 4. 限制

- 无 IPv4-mapped IPv6 归一化；
- 代理/NAT 后 peer 为代理地址；
- 仍无 TLS。
