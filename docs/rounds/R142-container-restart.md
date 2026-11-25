# R142 · 容器重启持久性

> 日期：2026-11-25  
> 结果：Docker Compose 重启 broker 后，消息仍可拉取（1/1）。

---

## 1. 步骤

```text
docker compose -f deploy/docker-compose.lite.yml up -d
AclSmoke -> SEND_OK msgId=7F...3D440000
docker compose restart mq-rust-broker
PullOnly MQ_CT_RESTART 1
```

## 2. 结果

```text
PULL_ONLY received=1 expected=1

/stats:
messages=34
topics=6
MQ_CT_RESTART messages=1
commitlog_bytes=7066
```

## 3. 结论

- broker 重启后从 `/data` 卷恢复 CommitLog/CQ/offset；
- 宿主机端口映射与 namesrv 路由保持可用；
- TC 容器未重启，事务协调状态不受影响。

## 4. 限制

- 仅单 broker；
- 未验证容器删除/volume 重建；
- K8s 持久卷重启未复测。
