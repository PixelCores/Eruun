# Helm 部署契约

> 状态：Current。本文说明 `deploy/helm/eruun` 的当前安装参数、运行探针和 Kubernetes 权限边界。

## 安装前提

Chart 部署 Eruun 的 API、Controller、Scheduler、Worker 四类运行角色，以及 MySQL 和 Redis。默认密码是占位符，安装时必须通过受控 values 文件提供真实值，不要把密码直接写入命令历史或提交到仓库。

```yaml
# secure-values.yaml（示例结构；不要提交真实值）
mysql:
  rootPassword: "<strong-password>"
redis:
  password: "<strong-password>"
```

```bash
helm upgrade --install eruun deploy/helm/eruun \
  --namespace eruun-system \
  --create-namespace \
  --values secure-values.yaml
```

`deploy/all_in_one_install_quickstart.sh` 使用同一套四角色拓扑。Helm 安装时，Quickstart 把密码写入权限为 `0600` 的临时 values 文件并在退出时清理；manifest 安装时使用 `deploy/eruun-stack.yaml` 中的四类 Deployment。

manifest 安装会先用 server-side dry-run 验证四角色清单，再把旧单进程 `Deployment/eruun` 缩容到零并保留为回滚点。四类 Deployment 全部 ready 后才清理旧 Deployment 和 `ServiceAccount/eruun-platform`；应用、覆盖或 readiness 失败时会删除本轮角色 Deployment 并恢复旧副本数。旧 ServiceAccount 清理失败只记录告警，不会把已经 ready 的新运行时判为安装失败；迁移过程不会删除 MySQL、Redis 或其持久化数据。

## 运行契约

- API 监听 `0.0.0.0:<service.port>`，默认 Service 和容器端口都是 `8000`；Service 只选择 API Pod。
- Controller 和 Scheduler 分别竞争 `runtime.controllerLockName`、`runtime.schedulerLockName` 指定的 Lease。
- readiness/liveness 路由为 `/api/v1/readyz` 和 `/api/v1/healthz`。
- 四类 Deployment 和 ServiceAccount 使用 `-api`、`-controller`、`-scheduler`、`-worker` 后缀。
- Quickstart readiness 会等待四类 Deployment、MySQL 和 Redis；名称计算与 Chart 的 63 字符截断和角色后缀预留规则一致。
- 将 `FULLNAME_OVERRIDE` 显式设为空时，Quickstart 使用 `<release>-eruun`；非空 `SERVICE_NAME` 只覆盖 Service 查询、port-forward 与打印命令。

```yaml
runtime:
  controllerLockName: eruun-controller
  schedulerLockName: eruun-scheduler
  heartbeatInterval: 10s
  leaseDuration: 30s
  leaseReaperInterval: 10s
  workerDrainTimeoutSeconds: 60
  terminationGracePeriodSeconds: 90
  roles:
    api:
      replicas: 2
    controller:
      replicas: 2
    scheduler:
      replicas: 2
    worker:
      replicas: 3
```

generation/token ownership 与数据库 execution lease 是唯一执行协议，不提供关闭开关。当前处于开发阶段，部署前应使用新数据库，或清理旧任务数据和消息积压；不支持缺少 ownership 字段的历史任务、消息或混合版本 Worker。

每类副本数大于 1 的角色默认生成 `maxUnavailable: 1` 的 PodDisruptionBudget。角色资源名会先为后缀预留长度再限制为 63 字符，因此长 `fullnameOverride` 下仍保持唯一。topology spread 默认按 `kubernetes.io/hostname` 分散同角色 Pod。

所有运行角色默认使用 90 秒 `terminationGracePeriodSeconds`，覆盖 60 秒 Worker drain；终止宽限期必须严格大于 `workerDrainTimeoutSeconds`。每个角色都使用最长 150 秒的 `startupProbe` 窗口，首次安装的 API schema 初始化或 Worker informer cache 同步期间不会提前触发 liveness 重启。Controller、Scheduler 和 Worker 副本数可以独立调整，不要求奇数。

`env` 只用于追加应用配置，不允许设置由 Chart 管理的 `ERUUN_ROLE`、`ERUUN_ID`、`ERUUN_DATASTORE_SCHEMA_MODE` 或 `ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT`。Worker drain 必须通过 `runtime.workerDrainTimeoutSeconds` 配置，这样 Chart 才能同时校验 `terminationGracePeriodSeconds`。旧单进程顶层键 `replicaCount` 和 `resources` 会被 schema 拒绝；副本数与资源必须分别配置在 `runtime.roles.<role>.replicas` 和 `runtime.roles.<role>.resources`。

数据库 schema 的写入所有权与普通运行时启动分离：

- 首次 `helm install` 时只有 API Deployment 使用 `migrate` 模式；其他角色使用 `validate`。多 API 副本仍通过 MySQL 命名锁串行迁移，避免 Chart 内置 MySQL 尚未创建时运行 `pre-install` hook。
- `helm upgrade` 会先运行一个 `pre-upgrade` migration Job，使用 `migrate-only` 模式完成 AutoMigrate、数据回填和旧表/列清理；升级后的四类运行角色全部只做 schema 校验。
- migration Job 与四类 Deployment 共用数据库环境变量默认值，并按原顺序追加 `env`。通过 `env` 覆盖 `ERUUN_DATASTORE_URL` 时，Job 和运行 Pod 使用同一外部数据库；DSN 中的 `$(MYSQL_PASSWORD)`、`$(MYSQL_DATABASE)` 或此前定义的自定义环境变量引用也保持相同顺序。未覆盖时使用 Chart 内置 MySQL Secret。
- migration Job 不挂载 Kubernetes ServiceAccount token，也不初始化 Kubernetes、消息队列、HTTP 或业务服务。它只在全部结构与数据迁移完成后写入版本化 migration marker；运行角色在 marker、必需表或列缺失时 fail-fast，不会偷偷修改 schema。
- 直接运行二进制的默认值保持 `migrate`；静态 `deploy/eruun-stack.yaml` 明确让 API 使用 `migrate`，其余三类角色使用 `validate`。

由于 `pre-upgrade` hook 执行时旧版本 Pod 仍可能提供服务，每次数据库变更都必须遵循可滚动升级的 expand/backfill/contract 顺序。破坏旧版本读写的删列、改义或不可逆数据转换不得直接放进一次在线升级；应拆分为多个版本，并在旧代码不再运行后执行 contract 阶段。

Chart 内置 MySQL 和 Redis 是单副本开发依赖，不构成企业 HA 数据面。生产环境应使用外部 HA MySQL、HA Redis 或多 Broker Kafka，并通过受控 Chart overlay 接入。

## Adopted import keyring

显式 adopted 接管使用 AES-256-GCM 保存导入 Secret，并使用 HMAC 签发导入与 cleanup 计划指纹。启用 adopted API 前，必须预先创建包含完整 keyring JSON 的 Kubernetes Secret：

```yaml
importSecretKeyring:
  existingSecret: eruun-import-keyring
  key: keyring.json
```

Chart 只把该 Secret 挂载到实际使用 keyring 的 API 和 Worker，路径为 `/var/run/secrets/eruun/import-secret-keyring/keyring.json`，并设置 `ERUUN_IMPORT_SECRET_KEYRING_FILE`。内联配置与文件配置互斥；同时存在或 keyring 无法解析时进程启动失败。未设置 `existingSecret` 时不渲染对应 env、volume 或 volumeMount。

每个 key 值必须是恰好 32 字节密钥的 Base64 编码。

```json
{
  "activeKeyId": "2026-08",
  "keys": {
    "2026-08": "<base64-32-byte-key>",
    "2026-07": "<previous-key-during-rotation>"
  }
}
```

## ServiceAccount 与 RBAC

| Value | 默认值 | 语义 |
| --- | --- | --- |
| `serviceAccount.create` | `true` | 创建四个角色专用 ServiceAccount。 |
| `serviceAccount.roleNames.api` | `""` | 不创建账号时使用的已有 API ServiceAccount。 |
| `serviceAccount.roleNames.controller` | `""` | 不创建账号时使用的已有 Controller ServiceAccount。 |
| `serviceAccount.roleNames.scheduler` | `""` | 不创建账号时使用的已有 Scheduler ServiceAccount。 |
| `serviceAccount.roleNames.worker` | `""` | 不创建账号时使用的已有 Worker ServiceAccount。 |
| `serviceAccount.annotations` | `{}` | 写入 Chart 创建的 ServiceAccount。 |
| `serviceAccount.automountServiceAccountToken` | `true` | 控制 Chart 创建账号的 token 自动挂载。 |
| `rbac.create` | `true` | 创建 Lease Role/RoleBinding、资源管理 ClusterRole 及 Controller 专用 ClusterRole。 |

Controller 和 Scheduler 绑定 namespace-scoped Leader Election Role；API 与 Worker 绑定资源管理 ClusterRole；Controller 的专用角色允许 Pod `get/list/watch/patch/delete`、Pod 日志 `get`、Job `get/create/update/delete` 和 ReplicaSet `get`；Scheduler 不绑定任何 ClusterRole。Controller 除了 Pod 观察和 adopted metadata 标签协调，还运行延时 Job 分发、结果处理和 outbox 恢复，因此需要创建/复用 Job、收集日志及清理已完成的 Job/Pod。使用已有账号时，Controller 名称必须不同于 API/Worker，Scheduler 名称必须不同于其余三类角色，防止共享身份重新扩大权限；API 与 Worker 可以有意复用同一个资源管理身份。

默认资源管理 ClusterRole 只包含当前 Eruun 管理 Kubernetes 工作负载、Pod 日志与 exec、Namespace、Service/Secret/ConfigMap/PVC、StorageClass、Ingress 和 RBAC Trait 所需的显式资源权限，不绑定内置 `cluster-admin`。Pods 的 `patch` 只用于 adopted source owner 链校验成功后补 metadata 管理标签；Controller 专用角色不授予 Secret 读取、Pod exec、Deployment/StatefulSet 管理或 RBAC 管理权限。Job `update` 用于复用未完成 Job 时更新 task/execution key/run generation，支持 Worker 接管新的执行代次。资源管理角色中的 ReplicaSet `update` 只用于 signed cleanup quiesce，ReplicaSet/ControllerRevision `delete` 只用于签名计划覆盖且 UID 匹配的 runtime child。PV 与 HPA 权限保持只读；PDB、NetworkPolicy 和 PVC update 用于 source-aware 调和。RBAC Trait 所需的 `roles`、`clusterroles` 规则包含 Kubernetes 要求的 `bind`、`escalate` verbs。

Chart 不授予 CRD/custom resource、impersonation、Pod attach 或 Pod port-forward 权限。静态 `deploy/eruun-stack.yaml` 使用与 Chart 相同的显式资源管理/Controller 权限边界，不再绑定 `cluster-admin`。Quickstart 仅在 `INSTALL_MODE=manifest`、`NAMESPACE=eruun-system` 且 `MANIFEST` 指向脚本同目录的默认 `eruun-stack.yaml` 时，在新权限对象成功应用后删除旧版本遗留的固定 Binding `eruun-platform-cluster-admin`；不存在时无操作，dry-run 只预览删除。Helm 安装、自定义 manifest 路径或其他 namespace 不自动清理该绑定，避免撤销其他实例的权限；如需迁移这些安装方式，应先确认旧绑定的全部 ServiceAccount 已获得替代权限，再由管理员删除旧绑定。

ClusterRole 名称完整使用 `<release fullname>-<release namespace>`，Controller 角色追加 `-controller-observer`，避免不同 namespace 中同名 release 争用同一个集群级对象。ClusterRole 和 ClusterRoleBinding 使用 Kubernetes RBAC path-segment 名称规则，不受通用 DNS label 的 63 字符限制。

```bash
helm upgrade --install eruun deploy/helm/eruun \
  --namespace eruun-system \
  --set serviceAccount.create=false \
  --set serviceAccount.roleNames.api=precreated-eruun-api \
  --set serviceAccount.roleNames.controller=precreated-eruun-controller \
  --set serviceAccount.roleNames.scheduler=precreated-eruun-scheduler \
  --set serviceAccount.roleNames.worker=precreated-eruun-worker \
  --set rbac.create=false \
  --values secure-values.yaml
```

当 `serviceAccount.create=false` 时必须提供全部四个 `serviceAccount.roleNames.*`，并满足上述身份隔离约束。也可以保留 `rbac.create=true`，让 Chart 为已有 ServiceAccount 创建绑定。

## 静态验证

```bash
helm lint deploy/helm/eruun --values secure-values.yaml
helm template eruun deploy/helm/eruun --namespace eruun-system --values secure-values.yaml
bash deploy/helm/eruun/helm_template_test.sh
```

静态渲染只能验证对象结构和引用闭环。正式发布前仍应在隔离集群执行 rollout、健康探针和代表性的 `kubectl auth can-i --as=system:serviceaccount:<namespace>:<name>` 检查。
