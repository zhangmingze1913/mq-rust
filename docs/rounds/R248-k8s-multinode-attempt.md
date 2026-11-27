# R248 · 多节点 K8s 4 broker（未完全通过）

> 日期：2026-11-27  
> 结果：4 节点 kind 因 CNI/资源失败；退化为 3 节点 kind，4 broker 分布在 2 个 worker，namesrv 路由可用，但 in-cluster SDK 在宿主饱和下超时，未取得 15/15。

---

## 1. 4 节点 kind

```text
kind-4node.yaml（control-plane + 3 worker）
kindnet 在部分节点 CreateContainerError
节点 NotReady，4 broker 未完成调度
```

## 2. 退化 3 节点

```text
kind 3 节点
4 broker 调度到 worker/worker2（2 节点）
namesrv 日志可见 MQ_MULTI 路由
```

## 3. 问题

```text
mq-multi: RemotingTimeoutException wait response on namesrv
mq-multi2: send request to 10.96.x.x:9877 failed
```

宿主 9GB/3CPU 下，4 brokers + namesrv + kind + JVM 过载，namesrv 客户端请求超时；非协议错误。

## 4. 产物

- `deploy/k8s/kind-4node.yaml`；
- `deploy/k8s/raft4.yaml` 支持 required anti-affinity（4 节点时一节点一 broker）；
- 3 节点回退使用 preferred anti-affinity。

## 5. 结论

- 跨节点 4 broker 未取得有效 SDK 证据；
- 需要 ≥4 节点、≥12–16GB 可用内存或更多 CPU；
- 本轮不宣称多节点 4 broker 通过。
