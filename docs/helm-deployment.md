# Helm 部署契约

> 状态：Current。本文说明 `deploy/helm/eruun` 的当前安装参数、运行探针和 Kubernetes 权限边界。

## 安装前提

先按 [账号与空间部署文档](account-auth-workspaces.md) 创建 `eruun-account-config` Secret，并设置 `auth.existingSecret=eruun-account-config`；`auth.key` 默认为 `accounts.json`。所有节点只读挂载该 JSON。脚本安装须指定 `AUTH_CONFIG_FILE`。需要支持 NetworkPolicy 的 CNI 和 Restricted v1.34 Pod Security Admission，正确填写集群网络范围；注册/保存应用不会创建 namespace。

独立 `command` Job 不需要附加配置。启用 Harbor 评测时，另创建含 `jobs.json` 的 Secret，通过 `jobs.existingSecret`（及可选 `jobs.key`）挂载到所有节点；配置 Runner 镜像、平台 API 地址、精确 API 出站规则与可选 MinIO 连接，见 [空间 Job 部署](workspace-jobs-api.md#管理员配置与部署)。Chart 不创建此 Secret 或发布 Runner 镜像。新增制品表和任务规格字段仍由既有 schema 迁移流程管理。

Chart 部署一个统一 runtime Deployment，默认 4 个节点（1 Leader＋3 Worker），以及 MySQL 和 Redis。默认密码是占位符，安装时必须通过受控 values 文件提供真实值，不要把密码直接写入命令历史或提交到仓库。

```yaml
# secure-values.yaml（示例结构；不要提交真实值）
mysql:
  rootPassword: "<strong-password>"
redis:
  password: "<strong-password>"
```

```bash
helm upgrade --install eruun deploy/helm/eruun \
  --set-string auth.existingSecret=eruun-account-config \
  --namespace eruun-system \
  --create-namespace \
  --values secure-values.yaml
```

`deploy/all_in_one_install_quickstart.sh` 使用同一套统一节点拓扑。Helm 安装时，Quickstart 把密码写入权限为 `0600` 的临时 values 文件并在退出时清理；manifest 安装时使用 `deploy/eruun-stack.yaml` 中的统一 Deployment。

再次运行 Quickstart 会读取并复用当前安装的 MySQL、Redis 凭据；如果 Secret 缺失、字段损坏或持久资源与 Secret 不一致，安装会停止，不会为已有持久卷生成新密码。Chart 内置 MySQL 的 `rootPassword`、`database`、`servicePort`、资源 fullname，以及 Redis 的 `password`、`servicePort` 和资源 fullname 在已有工作负载上不可直接更改；Helm upgrade 会在 hook 前拒绝这类变化。首次安装可通过 `mysql.servicePort` 和 `redis.servicePort` 设置进程及 Service 的监听端口。需要轮换凭据、改库名或服务地址时，应先使用独立的数据迁移/凭据轮换流程同步更新数据库、缓存和 Secret，再执行升级。

内置 MySQL、Redis 的凭据 Secret 与 PVC 使用相同的持久化生命周期：Chart 通过 `helm.sh/resource-policy: keep` 在 `helm uninstall` 后保留这两个 Secret，用于识别原数据卷、阻止 fullname 漂移并支持后续人工恢复。彻底销毁某次安装的数据时，操作者应在确认不再需要恢复后，显式删除该 release 对应的 MySQL/Redis PVC 和同名 Secret；只删除 Secret 会使保留的数据卷无法由 Chart 安全重装。

## 从旧四角色迁移

**这是需要维护窗口的拓扑迁移，不支持新旧混跑。** 旧 API、Controller、Scheduler、Worker 使用双 Lease，新节点使用单 Lease；旧 leader 不会被新 Lease 排除。

1. 备份现有 values、Secret 引用、部署清单及数据库；记录镜像版本与任务状态。移除旧角色、双 Lease、失主退出及以 PodName 固定 `ERUUN_ID` 的配置，将副本/资源迁移到 `runtime.replicas`、`runtime.resources`，ServiceAccount 改为统一身份。
2. 停止外部新提交，保留旧运行时让在途长任务完成或按已有恢复契约排空。默认 60 秒进程 drain 不是长评测的完成保证；安排足够维护时间，不删除任务/消息来绕过排空。
3. 将旧四类 Deployment 全部缩容为零，确认旧 Pod、控制循环及执行者已停止，再执行新版本安装/升级。Helm 与 Quickstart 都会检查旧 Deployment 的期望、实际及终止中副本，并等待条件要求旧角色 Pod 已全部删除；未满足时拒绝继续，不自动替操作者关闭业务。
4. 完成 schema 迁移后启动新节点；核验唯一 Lease holder、Service 指向该 Leader、其他 Worker 的 readiness，再恢复流量。HTTP/gRPC 和 port-forward 连接需要重建。
5. 回滚前同样先停止新拓扑，核对数据库 schema 和任务格式能否由旧版本读取，再恢复旧部署；不能只回退镜像并让两套选举并行运行。

不得在迁移中删除 MySQL/Redis PVC 或重置凭据。下面 schema hook 说明不表示本次拓扑升级能够在线滚动完成。

## 阶段一 Sandbox 与容量实验

新 eval 使用按需 `agents.kruise.io/v1alpha1 Sandbox`；先安装并核验 ACK/ACS Agent Sandbox 能力，Chart 不安装云厂商控制器。统一节点都可能接任 Leader，因此其身份包含 Sandbox API/维护和 Worker 执行所需权限；任务环境仍使用空间默认 ServiceAccount 且不挂载 Token。数据库先迁移 JobSandbox、创建速率预算表和 Job 资源快照字段，再按 [版本与拓扑升级边界](workspace-jobs-api.md#管理员配置与部署) 部署；已有 v1 Runner 和无 claim 的更旧协议需分别处理。

两个统一节点（一个 Leader、一个 Worker）固定资源的覆盖 values 与手动命令见 [容量阶梯](../examples/agent-evaluation/load-test/README.md#固定单-worker-的手动容量阶梯)。默认每 Worker 100 controller、全局准入 100、每空间 10、创建 5 QPS/突发 10、启动中 Sandbox 100 均是初始值，不是万级容量配置或副本建议。通过 `env` 传入 `ERUUN_WORKFLOW_LEASE_REAPER_BATCH_SIZE` 可调回收批次；与全局 scheduler 策略、真实 quota、Worker 资源和镜像拉取能力共同测量。实际部署和放量由操作者执行。

## 运行契约

- 所有节点使用一个 Deployment 和 ServiceAccount，默认名称 `<fullname>-runtime`。`runtime.replicas` 默认 4，`runtime.resources` 是每个节点的统一资源配置；单节点没有新任务 Worker 容量，生产至少 2 个、默认建议 4 个；Chart 和 Quickstart 副本配置拒绝小于 2。
- Leader 提供业务 HTTP 和 gRPC，默认端口为 8000 / 9000。固定 Service 的 selector 使用 `app.kubernetes.io/component=runtime`、`app.kubernetes.io/instance=<release>` 和 `eruun.io/runtime-id`；初值 `unassigned`，Leader 以自身 Pod UID 作为 runtime-id 更新选择。EndpointSlice 由 Kubernetes 原生维护。
- `runtime.leaderLockName` 可显式指定 Lease 名，留空时 Chart 按 release fullname 生成 `<fullname>-runtime`；二进制 `--leader-lock-name` 默认 `eruun-runtime`。集群用 Downward API 把 PodName 注入 `ERUUN_POD_NAME`，不设置 `ERUUN_ID`，使 Lease identity 保持每进程 UUID；`ERUUN_LEADER_SERVICE_NAME` 指向固定 Service。
- 每个节点均有 `/api/v1/healthz` 与 `/api/v1/readyz`。readiness 反映当前职责与依赖，健康 Worker 也为 ready；不能把所有 ready Pod 都加入业务 Service。
- 失主停止 API/控制职责后重入 Worker；升主只停止新接单，已有任务继续完成并续租。切主会中断连接，`kubectl port-forward svc/<service>` 需要重启；客户端先核对任务状态再安全重试。
- Quickstart 等待统一 runtime Deployment 及 MySQL/Redis。`FULLNAME_OVERRIDE` 为空时使用 `<release>-eruun`；覆盖 Service 名称时须确保运行配置与实际 Service 一致。

```yaml
runtime:
  replicas: 4
  leaderLockName: ""  # Chart derives a release-scoped Lease name
  heartbeatInterval: 10s
  leaseDuration: 30s
  leaseReaperInterval: 10s
  workerDrainTimeoutSeconds: 60
  terminationGracePeriodSeconds: 90
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: "1"
      memory: 1Gi
```

资源只是示例，须结合当前 Leader 的 API/控制负载和 Worker 容量测量。副本数不要求奇数；增加节点增加候选和 Worker 容量，不增加活跃 Leader 数。顶层 `podDisruptionBudget` 控制统一 runtime PDB，topology spread 按统一节点分散；PDB 不保证始终存在 Leader，也不能替代故障恢复验证。

所有节点默认 90 秒 `terminationGracePeriodSeconds`，须严格大于 60 秒 `workerDrainTimeoutSeconds`。startupProbe 给 schema 初始化和 informer initial sync 留出启动窗口。`env` 不能覆盖 Chart 管理的实例 ID、Leader 入口、schema、监听及 drain 配置；drain 应通过对应 runtime value 设置，避免退出预算不一致。

已移除 `runtime.roles`、`serviceAccount.roleNames`、旧角色/双 Lease/失主退出参数。不要用 `--reuse-values` 直接携带旧结构升级；以新 values 为基准迁移所需设置，避免丢失外部数据库、镜像、Secret 和配额配置。generation/token ownership 始终启用，不提供关闭开关。

数据库 schema 与普通服务职责分离：

- 首次 Helm 安装，所有节点使用 `migrate`，由 MySQL 命名锁串行执行初始化。
- Helm 升级先通过 `pre-upgrade` Job 使用 `migrate-only` 完成迁移，随后 runtime 节点使用 `validate`。Job 不启动 Kubernetes、消息或业务 API 运行时。
- Job 与节点使用相同 DSN/env 配置；迁移成功后写版本 marker，校验模式检查 marker、表和列，不隐式迁移。
- 独立二进制默认 `migrate`。显式分阶段部署时先迁移，再以 `validate` 启动节点；不要以同时运行不兼容旧服务来完成这次拓扑切换。

旧 `--datastore-database` / `ERUUN_DATASTORE_DATABASE` 已移除，即使旧 env 为空也须删除；数据库名由 `--datastore-url` / `ERUUN_DATASTORE_URL` 的 DSN 指定。Chart 的 `mysql.database` 保持现有含义。

内置 MySQL/Redis 是单副本开发依赖。生产需要外部 HA 数据库/缓存或多 Broker Kafka、备份恢复验证与受控接入配置；同构节点不自动提供数据面 HA。

## Adopted import keyring

显式 adopted 接管使用 AES-256-GCM 保存导入 Secret，并使用 HMAC 签发导入与 cleanup 计划指纹。启用 adopted API 前，必须预先创建包含完整 keyring JSON 的 Kubernetes Secret：

```yaml
importSecretKeyring:
  existingSecret: eruun-import-keyring
  key: keyring.json
```

Chart 把该 Secret 挂载到所有统一节点，路径为 `/var/run/secrets/eruun/import-secret-keyring/keyring.json`，并设置 `ERUUN_IMPORT_SECRET_KEYRING_FILE`。内联配置与文件配置互斥；同时存在或 keyring 无法解析时进程启动失败。未设置 `existingSecret` 时不渲染对应 env、volume 或 volumeMount。

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
| `serviceAccount.create` | `true` | 创建统一 runtime ServiceAccount |
| `serviceAccount.name` | `""` | 指定账号名称；不创建时必须填写已有账号 |
| `serviceAccount.annotations` | `{}` | 创建账号的 annotations |
| `serviceAccount.automountServiceAccountToken` | `true` | token 自动挂载 |
| `rbac.create` | `true` | 创建统一运行节点需要的 Role、ClusterRole 及绑定 |

每个节点都可能当选，运行身份必须包含 Lease、入口 Service/Pod 标签更新、API 管理、Controller 观察/结果维护以及 Worker 执行权限。不能继续把旧 Scheduler 的最小权限直接用于新节点。权限以渲染清单中的显式规则为准，不绑定内置 `cluster-admin`；业务空间的受限 ServiceAccount、授权检查、执行身份及 UID 清理约束仍保留。

账号、Jobs 和可选 import keyring Secret 挂载到所有节点，统一配置支持接任。扩大到统一运行身份是部署取舍，不应再宣称 API/Controller/Worker 之间存在静态 RBAC 隔离。

```bash
helm upgrade --install eruun deploy/helm/eruun \
  --set-string auth.existingSecret=eruun-account-config \
  --namespace eruun-system \
  --set serviceAccount.create=false \
  --set serviceAccount.name=precreated-eruun-runtime \
  --set rbac.create=false \
  --values secure-values.yaml
```

已有账号必须先获得本版本所需权限；也可保留 `rbac.create=true` 让 Chart 创建绑定。旧 ClusterRoleBinding 的删除属于迁移清理，须确认没有其他实例使用，不能为了缩减角色直接撤销共享权限。

## 静态验证

```bash
helm lint deploy/helm/eruun --values secure-values.yaml
helm template eruun deploy/helm/eruun --namespace eruun-system --values secure-values.yaml
bash deploy/helm/eruun/helm_template_test.sh
```

静态渲染只能验证对象结构和引用闭环。正式发布前仍应在隔离集群执行 rollout、健康探针和代表性的 `kubectl auth can-i --as=system:serviceaccount:<namespace>:<name>` 检查。
