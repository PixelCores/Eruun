# ingress-nginx TCP 暴露 Redis 和 MySQL

## 背景与需求

用户需要通过域名访问项目部署清单中的 Redis 和 MySQL。现有 `eruun` 已通过 HTTP Ingress 暴露 API，但 Redis/MySQL 是原生 TCP 协议，不能使用标准 Kubernetes Ingress 的 HTTP 路由规则。

## 影响范围

- API: 无。
- Domain: 无。
- DB: MySQL 对外访问方式增加运维配置示例。
- Cache: Helm chart 中 Redis 改为密码保护，并把 apiserver Redis 配置接到同一 Secret。
- K8s: 新增外部 ingress-nginx Helm values 示例，在 `eruun-stack.yaml` 中加入 `ingress-nginx/tcp-services` ConfigMap，并按用户要求提供 Redis/MySQL 标准 Ingress host 模板。
- Workflow: Redis 仍作为 cache、messaging 和 locker 后端使用。

## 技术选型与取舍

选择 `ingress-nginx` TCP services 配置作为原生连接主路径。标准 Ingress 只适合 HTTP/HTTPS；Redis/MySQL 需要 TCP 端口转发。按用户要求，manifest 仍提供 Redis/MySQL 标准 Ingress 模板，方便安装方替换域名。

没有让 Eruun Helm chart 直接创建 `ingress-nginx/tcp-services` ConfigMap，因为该对象属于 ingress-nginx 控制器级共享配置，直接接管可能覆盖其他 TCP 映射。Manifest 模式按用户期望把映射写入 `eruun-stack.yaml`，因此使用前需要确认该 ConfigMap 没有其他 owner。

没有把 Redis/MySQL Service 改成 LoadBalancer 或 NodePort，避免改变 Eruun 默认集群内服务形态；外部访问由 ingress-nginx release 显式配置。

## 实现摘要

- 新增 `deploy/ingress-nginx-tcp-values.yaml`，提供 3306 和 6379 到 Eruun Service 的 TCP 映射。
- `deploy/eruun-stack.yaml` 新增 `ingress-nginx/tcp-services` ConfigMap，便于 manifest 安装路径直接包含 TCP 映射。
- `deploy/eruun-stack.yaml` 新增 `eruun-mysql`、`eruun-redis` 标准 Ingress 模板，host 默认为 `mysql.example.com` 和 `redis.example.com`，由安装方替换。
- apiserver Deployment 显式注入 `ERUUN_CACHE_HOST`、`ERUUN_CACHE_PORT`、`ERUUN_CACHE_PASSWORD` 和 `ERUUN_MSG_TYPE`。
- 新增当前行为文档并链接到 `docs/README.md`。

## 测试与验收

- 静态检查新增 ingress-nginx values 包含 `3306` 和 `6379` 映射。
- Helm 渲染应验证 Redis Secret、Redis 认证启动参数、apiserver Redis 环境变量。

## 风险与后续

- TCP 转发按端口匹配，不按域名分流；两个域名可以指向同一个入口，但实际区分靠 3306/6379。
- 对外暴露数据库存在安全风险，生产环境应使用内网入口、源 IP 限制和强密码。
- 如果目标集群 ingress-nginx 不是官方社区 chart，需要按文档手动配置 ConfigMap、controller 参数和 Service 端口。
