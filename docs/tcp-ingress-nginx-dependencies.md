# Redis 和 MySQL TCP 外部访问

> 状态：Current。本文说明如何通过 Kubernetes Service 直接暴露 Eruun 内置 Redis 和 MySQL 的原生 TCP 端口。

## 行为边界

Kubernetes `networking.k8s.io/v1 Ingress` 默认处理 HTTP/HTTPS 路由，不适合 Redis 和 MySQL 原生 TCP 协议。`deploy/eruun-stack.yaml` 只为 `eruun` API 保留 HTTP Ingress；MySQL 和 Redis 外部访问使用独立 `LoadBalancer` Service 暴露原生 TCP 端口。

本项目采用独立 Service 暴露端口：

- MySQL: `mysql.eruun.example.com:13306`
- Redis: `redis.eruun.example.com:16379`

DNS 需要指向对应 Service 的外部地址，或指向把这些端口转发到对应 Service 的外部负载均衡/NAT 地址。

## Manifest 配置

`deploy/eruun-stack.yaml` 保留内部 headless Service，供 StatefulSet 和 Eruun API Server 使用：

```yaml
kind: Service
metadata:
  name: eruun-mysql
spec:
  clusterIP: None
  ports:
    - name: mysql
      port: 3306
      targetPort: mysql
```

同时新增外部访问 Service：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: eruun-mysql-external
  namespace: eruun-system
spec:
  type: LoadBalancer
  ports:
    - name: mysql
      port: 13306
      targetPort: mysql
      protocol: TCP
  selector:
    app: eruun-mysql
```

```yaml
apiVersion: v1
kind: Service
metadata:
  name: eruun-redis-external
  namespace: eruun-system
spec:
  type: LoadBalancer
  ports:
    - name: redis
      port: 16379
      targetPort: redis
      protocol: TCP
  selector:
    app: eruun-redis
```

Eruun 应用内部仍连接 `eruun-mysql:3306` 和 `eruun-redis:6379`，不要把内部 Service 端口改成 `13306` 或 `16379`。

## DNS 和客户端验证

DNS 需要把以下记录指向对应的外部地址：

- `mysql.eruun.example.com`
- `redis.eruun.example.com`

验证 Service：

```bash
kubectl -n eruun-system get svc eruun-mysql-external eruun-redis-external
```

DataGrip MySQL 连接参数。安装脚本会在本地生成凭据，也可通过 `MYSQL_USER` 和 `MYSQL_PASSWORD` 显式提供：

```text
Host: mysql.eruun.example.com
Port: 13306
User: eruun
Password: __REPLACE_WITH_MYSQL_PASSWORD__
Database: eruun
```

验证 MySQL：

```bash
mysql -h mysql.eruun.example.com -P 13306 -ueruun -p"${MYSQL_PASSWORD}" eruun
```

Redis 连接参数：

```text
Host: redis.eruun.example.com
Port: 16379
Password: __REPLACE_WITH_REDIS_PASSWORD__
```

验证 Redis：

```bash
REDISCLI_AUTH="${REDIS_PASSWORD}" redis-cli -h redis.eruun.example.com -p 16379 ping
```

预期 Redis 返回 `PONG`。

## 安全要求

- 对公网暴露 Redis/MySQL 前，必须通过安装脚本、本地环境变量或受管 Secret 提供强密码。
- 优先使用内网 LoadBalancer、VPN、堡垒机或源 IP 白名单限制访问面。
- Redis/MySQL 原生连接不由 HTTP Ingress TLS 终止保护；如需加密链路，应在数据库协议层、专用代理或网络层处理。
- `ingress-nginx` 的 TCP ConfigMap 是控制器级共享配置。使用 `deploy/eruun-stack.yaml` 前，请确认该 ConfigMap 没有由 ingress-nginx Helm release 或其他团队管理，避免覆盖其他服务的 TCP 端口映射。
