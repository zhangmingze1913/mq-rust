# R136 · 分发包实机验证

> 日期：2026-11-25  
> 结果：解压 `dist/mq-rust-0.9.0-rc.1-win64.zip` 后直接运行，Compat 15/15、Tx commit=1。

---

## 1. 步骤

```text
Expand-Archive dist/mq-rust-0.9.0-rc.1-win64.zip
G:\MQ_soft\mq-rust-distcheck\mq-namesrv.exe :9887
G:\MQ_soft\mq-rust-distcheck\mq-broker.exe  :10931
```

## 2. 结果

```text
Compat：RESULT received=15 asyncOk=5 send=true
Tx    ：TX RESULT commit=1 rollback=0

/stats：
messages=16，topics=2（MQ_DIST2、MQ_DIST_TC2）
commitlog_bytes=3321
```

## 3. 包内容

```text
mq-broker.exe / mq-namesrv.exe / mq-tc.exe
README.md / CHANGELOG.md
15–21 文档
deploy/
VERSION.txt / SHA256SUMS
```

VERSION.txt：

```text
MQ-rust 0.9.0-rc.1 (win64)
Status: release candidate, not production 1.0
```

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       81 项通过
```

## 5. 说明

- 打包二进制 SHA 与 `target/release` 一致；
- 首次 Java 连接偶发空地址重试，重试即成功（客户端启动时序）；
- 未在干净机器验证 VC++ 运行库依赖。
