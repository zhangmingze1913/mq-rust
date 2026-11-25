# R97 · 提升后旧主 Fence 验证

> 日期：2026-11-25  
> 结果：旧 master 重启后读取 namesrv 路由发现已指向 10922，自动 `read_only=true`；新发送 10 条全部到新主。

---

## 1. 流程

```text
master 10921 + slave 10922（--promote-to 9171）
发送 50 条
kill master
slave 自动提升：route=10922, epoch=2
旧 master 重启：--fence-url 127.0.0.1:9171 --fence-interval-ms 1000
```

## 2. 结果

```text
旧 master /stats：
  messages=50, read_only=true

旧 master 日志：
  broker fence state changed route=127.0.0.1:10922 epoch=2 fenced=true

新 Producer 发送 10 条：0 失败
slave /stats：
  messages=60, read_only=false
```

## 3. 改动

- `/stats` 新增 `read_only` 字段；
- fence 轮询以 namesrv route 与 advertise 不一致即置只读；
- 旧主恢复后不再接受写。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 未验证旧主收到写请求时返回的具体错误码；
- fence 依赖 namesrv admin 可达；
- 无任期/选举，双主风险仍依赖外部编排。
