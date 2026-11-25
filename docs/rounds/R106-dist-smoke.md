# R106 · 分发包冒烟验证

> 日期：2026-11-25  
> 结果：解压 `mq-rust-0.9.0-rc.1-win64.zip`，用包内二进制启动，Compat/Batch/Tx 全部通过。

---

## 1. 验证

```text
zip SHA256:
9488B2114B5CE8DAEEF122C9057DF7F4DB50FB8E83D37F1F2D3E392D77508EB9

解压到 G:\MQ_soft\mq-rust-dist-test
启动 mq-namesrv.exe / mq-broker.exe

RESULT received=15 asyncOk=5 send=true
BATCH RESULT received=5
TX RESULT commit=1 rollback=0
```

## 2. 结论

- 包内二进制可直接运行；
- 文档/部署模板齐全；
- SHA256 与发布记录一致；
- 未依赖源码目录。

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       79 项通过
```

## 4. 限制

- 仅 Windows x64；
- 未签名；
- 仅冒烟 3 类场景。
