# R54 · 原生栈最终复验（R53 后）

> 日期：2026-11-25  
> 结果：5 端口全部 UP，SDK 15/15 通过。

---

## 1. 端口

```text
19877 namesrv
20921 broker
19191 tc
19090 metrics
19091 admin
```

## 2. SDK

```text
RESULT received=15 asyncOk=5 send=true
```

含 sync / async / oneway、Tag、Pull。

## 3. 说明

- 使用当前 release 二进制（含 Retention/栅栏/Admin 鉴权，但默认不启用令牌）；
- 原生脚本未传新参数，默认兼容；
- 停止脚本正常清理。

## 4. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       65 项通过
```

## 5. 限制

- 仅 Windows 原生脚本；
- 容器/K8s 仍未实测。
