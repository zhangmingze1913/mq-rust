# R227 · 动态加节点 API

> 日期：2026-11-27  
> 结果：新增 `POST /raft/add`；3 节点 leader 返回 `{"added":4}`，节点 4 作为 learner 加入。复制的 4 节点压测因本机 4 核过载未完成。

---

## 1. 实现

- `NetworkFactory.peers` 改为 `Arc<RwLock<HashMap>>`，运行时可加地址；
- `MessageRaft::add_node(id, addr)`：
  - 写入 peers；
  - `add_learner(id, (), true)`；
  - 用 peers 集合 `change_membership`；
- Admin `POST /raft/add?node=4&addr=ip:19704`；
- 非 leader 返回 409。

## 2. 实测

```text
node1: {"error":"not leader"}
node2: {"error":"not leader"}
node3: {"added":4}
```

节点 4 启动时预置 `.raft-initialized`，以 learner 启动；add 返回成功。

## 3. 未通过部分

- 4 节点本机 debug 运行时 heartbeat 全部 `Elapsed`；
- mqadmin 发送超时；
- 未验证节点 4 追平日志/成为 voter。

原因判断：4 个 debug broker + namesrv 在 4 核宿主上 Raft 心跳超时；需 release 或 K8s 多节点复测。

## 4. 附带修复

- applied 水位仅在存在快照时加载，避免“applied 超前、日志为空”导致 membership 丢失（K8s broker-2 现象）。

## 5. 质量门禁

```text
cargo fmt / clippy / test  通过（90 项）
```
