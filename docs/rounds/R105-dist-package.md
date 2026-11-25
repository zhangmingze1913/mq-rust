# R105 · Windows 分发包

> 日期：2026-11-25  
> 结果：`scripts/build-dist.ps1` 生成 `mq-rust-0.9.0-rc.1-win64.zip`（2.83MB），含二进制、文档、部署模板与 SHA256。

---

## 1. 包内容

```text
mq-broker.exe     2,328,064 B
mq-namesrv.exe    1,552,896 B
mq-tc.exe         2,454,016 B
README.md
15-交付清单.md
16-最终验收报告.md
deploy/
VERSION.txt
SHA256SUMS
```

## 2. 产物

```text
dist/mq-rust-0.9.0-rc.1-win64/
dist/mq-rust-0.9.0-rc.1-win64.zip
zip SHA256: 9488B2114B5CE8DAEEF122C9057DF7F4DB50FB8E83D37F1F2D3E392D77508EB9
```

（历史值；当前包 SHA 见 [20-交付索引](../20-交付索引.md)。）

## 3. 脚本

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build-dist.ps1 -Version 0.9.0-rc.1
```

- `cargo build --release --workspace`
- 复制 exe/docs/deploy
- 生成 `VERSION.txt` / `SHA256SUMS`
- `Compress-Archive`

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 5. 限制

- 仅 Windows x64；
- 未做代码签名；
- zip 不含 JRE/JDK 与 RocketMQ 发行包；
- dist/ 不纳入 Git（源码+脚本可重建）。
