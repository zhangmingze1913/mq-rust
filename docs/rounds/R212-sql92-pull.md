# R212 · SQL92 Pull 首心跳时序

> 日期：2026-11-26  
> 结果：SQL92 Pull 在 consumer 启动后等待首次心跳再拉取，过滤结果正确。

---

## 1. 根因

`PullAPIWrapper.pullKernelImpl` 依赖 `brokerVersionTable`，该表由心跳响应填充。  
consumer 启动后立即 pull 时版本仍为 0，客户端报：

```text
The broker does not upgrade to support for filter message by SQL92
```

不是 broker 过滤错误。

## 2. 修复

测试客户端在 `consumer.start()` 后等待 3s，让首次心跳完成，再执行 SQL92 pull。

## 3. 结果

```text
SQL92 RECV expensive price=150
SQL92 RESULT count=1
```

- `price=50` 被过滤；
- `price=150` 返回；
- 无 `does not upgrade` 异常。

## 4. 结论

- SQL92 Pull 功能可用；
- 客户端需等待首次心跳；
- 已在测试中固化该时序。

## 5. 质量门禁

```text
SQL92 Pull 测试通过
cargo fmt / clippy / test  通过（87 项）
```
