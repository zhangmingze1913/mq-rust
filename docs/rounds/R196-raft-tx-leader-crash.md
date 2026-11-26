# R196 · leader 在 half 阶段崩溃

> 日期：2026-11-26  
> 结果：事务 half 复制后 kill leader；新 leader 选出，half 未提交，集群继续接受新写入。

---

## 1. 步骤

```text
3 broker Raft，leader=10931
MqRustTxTimeoutSmoke 发送 half（本地事务 UNKNOW）
4s 后 kill leader 10931
等待 22s
```

## 2. 结果

```text
A/C /stats messages=0       # 无已提交消息
namesrv route: 10941 epoch=4 # 新 leader
mqadmin send MQ_R9_POST
SEND_OK
A/C /stats messages=1
```

- 旧 leader 中断后，half 未被提交；
- 新 leader 正常选出并发布路由；
- 后续普通写入成功。

## 3. 说明

- half 已通过 Raft 复制到 follower；
- leader 崩溃后，各副本本地超时任务回滚 half；
- 没有出现“客户端看到成功但消息丢失”的路径（本场景本地事务未提交）；
- 事务客户端自身在 leader 崩溃时可能收到异常，需要业务重试。

## 4. 边界

- 未验证 leader 在 commit 阶段崩溃的客户端重试幂等；
- 未做 check-back 与新 leader 的完整交互；
- 未做网络抖动/反复切换。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（85 项）
```
