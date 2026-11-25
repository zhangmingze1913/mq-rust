# R32 · 原生栈最终复验

> 日期：2026-11-25  
> 结果：`start-mq-rust.ps1` 修复 TC 端口参数后，5 端口全部 UP，SDK/事务/Inbox 全部通过。

---

## 1. 修复

`start-mq-rust.ps1` 原先依赖 `TC_LISTEN` 环境变量，但 `mq-tc` 当前 CLI 只认 `--api-listen`。  
已改为：

```powershell
mq-tc.exe --api-listen 127.0.0.1:19191
```

## 2. 复验

```text
port 19877: UP
port 20921: UP
port 19191: UP
port 19090: UP
port 19091: UP

RESULT received=15 asyncOk=5 send=true
TX RESULT commit=1 rollback=0
INBOX RESULT delivered=3 processed=1
```

## 3. 质量门禁

```text
cargo fmt --all -- --check   通过
cargo clippy --workspace --all-targets -- -D warnings  通过
cargo test --workspace       60 项通过
```

## 4. 限制

- 容器/K8s 仍未实测；
- 原生脚本仅 Windows；
- 性能/HA/安全缺口见 R30。
