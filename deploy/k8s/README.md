# Kubernetes 部署资产

本目录提供 MQ-rust 的 K8s 清单（**未在真实集群验证**；宿主无 Docker/K8s）。

## 文件

- `mq-rust.yaml`：Namespace + NameServer + Broker + TC StatefulSet（3 副本）

## 组件

| 资源 | 说明 |
|---|---|
| `mq-rust-namesrv` | Deployment + Service，9877 |
| `mq-rust-broker` | Deployment + Service，10921/9090/9091 |
| `mq-rust-tc` | Headless Service + StatefulSet 3 副本，19301/9191 |
| 存储 | Broker `emptyDir`（示例）；TC `volumeClaimTemplates` 1Gi |

## 部署

```bash
# 先构建镜像
docker build -f deploy/Dockerfile -t mq-rust:latest .

kubectl apply -f deploy/k8s/mq-rust.yaml
kubectl -n mq-rust get pods,svc
```

Broker 通过：

```text
--tc-url http://mq-rust-tc-0.mq-rust-tc:9191,...
--tc-reconcile-ms 2000
```

访问 TC。

## 验证

- YAML 解析：7 个文档，kind/name 正确；
- 未在真实 K8s 运行（环境限制）。

## 后续

- 镜像推送到私有 registry；
- Broker 使用 Local PV/CSI；
- 增加 HPA、PDB、NetworkPolicy、Prometheus ServiceMonitor；
- TC 滚动升级/故障切换演练。
