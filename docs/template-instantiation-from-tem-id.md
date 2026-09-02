# 基于 tmp.id 的应用模板实例化设计

> 状态：Implemented Reference。当前 `component[].tmp.id` 模板实例化能力已落地，本文保留设计背景、当前行为契约与后续演进问题。

> 术语边界：`component[].tmp.id` 是应用模板引用；`templateEnabled` / `tmp_enable` 是应用是否可被引用为模板的标记；storage trait 的 `tmpCreate` 只控制 PVC 创建模式，不是模板发现或模板启用标记。

## 背景与目标
- 支持用户在请求体中提供 `tmp:{id:{{app_id}}}`，以数据库 `eruun_applications` 中已有应用作为模板，快速创建多个相同形态的新应用（如多套 MySQL）。
- 在实例化过程中，用用户传入的组件 `name`（例：`fnlz2z1lxe85k3me66og`）覆盖模板中的组件名称及相关子字段（如存储名），并生成新的应用 `name`/`alias` 等元数据。
- 实例化结果写入数据库，新增的应用可选择成为“标准模板”：`eruun_applications` 表新增列 `tmp_enable`（bool，默认 false，对应 API 字段 `templateEnabled`），用来标记该应用是否允许被其他请求作为模板引用。
- 保证生成的资源可用、无冲突、可追溯，且操作幂等。

## 输入示例
```json
{
  "name": "fnlz2z1lxe85k3me66og-mysql",
  "alias": "mysql",
  "version": "1.0.0",
  "project": "",
  "description": "Create tmp Mysql",
  "component": [
    {
      "name": "fnlz2z1lxe85k3me66og",
      "tmp": { "id": "4tbupjg43ln3yj249l0v0fv8", "target": "mysql" }
    }
  ]
}
```

### init env 覆盖示例（未指定 name，默认命中第一个 init）
```json
{
  "name": "tenant-a-mysql-app",
  "namespace": "mysql",
  "alias": "tenant-a-mysql",
  "version": "1.0.7",
  "component": [
    {
      "name": "tenant-a-mysql",
      "type": "store",
      "tmp": { "id": "8oa5dohxupgwq4go3p1m2h0b", "target": "mysql" },
      "traits": {
        "init": [
          {
            "properties": {
              "env": {
                "MYSQL_DATABASE": "game",
                "SQL_URL": "https://paas-3os.oss-cn-shanghai.aliyuncs.com/uploads/2025/06/27/2506271630choUDT.sql",
                "MASTER_ROLE_NAME": "tmp-mysql-config-master",
                "SLAVE_ROLE_NAME": "tmp-mysql-config-slave"
              }
            }
          }
        ]
      }
    }
  ]
}
```

## 处理流程（当前契约）
1. 校验顶层字段：`name/alias/version` 必填；`component` 非空；`tmp.id` 必填且格式合法。
2. 查询模板：按 `tmp.id` 读取 `eruun_applications` 及其组件；校验模板状态可用且 `templateEnabled=true` 时才允许被引用（若为 false，则拒绝作为模板来源）。
3. 克隆模板：
   - 复制模板应用的结构（组件、traits、workload 配置等）。
   - 应用字段替换规则（见下）。
   - 对需要唯一性的字段生成新值（ID、端口、PVC 名、Service 名等）。
4. 唯一性检查：以顶层 `name` 等现有应用唯一约束检查重复；当前模板实例化请求不提供独立的显式 `idempotencyKey`。
5. 冲突检测：检查命名冲突（组件名、PVC/Service 名）、端口占用、项目/命名空间下资源配额。
6. 写入/创建：
   - 将“新应用”落库到 `eruun_applications`，默认 `templateEnabled=false`（落库列 `tmp_enable`），除非请求显式指定该实例应作为标准模板。
   - 写入新组件及关联资源，并触发后续创建流程；失败按现有应用创建错误语义返回。
7. 返回结果：返回新应用 ID、组件列表、来源模板信息（`templateId`、版本），以及新应用的 `templateEnabled` 状态。

## 字段替换规则
- 保留模板：镜像、配置 schema、必需的运行参数、trait 类型等模板定义本身。
- 使用用户应用元数据覆盖：应用 `name`/`alias`/`description`/`version`/`project`。
- 组件级覆盖（按传入组件 `name`）：
  - `component.name` 统一替换模板组件的 `name`。
  - 特征中的资源名（例：PVC/Storage 名、Service 名、Deployment 名称前缀）以新组件名为前缀/整体替换，保持命名约定。
  - 持久化存储 identity：顶层 `type=persistent` storage 是声明源；init/sidecar 中原始 `name`（`TrimSpace` 后）相同的 persistent storage 继承其 `name`、`tmpCreate`、`claimName`、`size` 与 `storageClass`，只保留自身挂载选项。这样同一逻辑存储只生成一个 PVC；多个容器可以各自将这个 volume 挂载到同一路径，但每个容器只会有一个对应的 `VolumeMount`。不同名称表示独立 PVC。
  - 存储创建策略：`tmpCreate=true` 视为使用 volumeClaimTemplates 风格创建 PVC（按重写后的存储名生成模板，并在主容器/init/sidecar 以同名卷挂载，`claimName` 可留空）；`tmpCreate=false` 使用 standalone PVC，实例化后优先使用显式 `claimName`，未提供时使用重写后的 `name` 作为 PVC 名；同名 PVC 已存在时部署阶段不更新其 spec。
  - 模板默认 StorageClass：请求侧可在 `component[].tmp.defaultStorageClass` 传入本次模板引用的默认 StorageClass。Eruun 会在模板 traits 解码后、资源名重写前，把该值递归写入 `traits.storage`、`traits.init[].traits.storage`、`traits.sidecar[].traits.storage` 中 `type=persistent` 且 `storageClass` 为空的 storage；模板已有显式 `storageClass` 不覆盖，非 persistent storage 不处理。
  - 若模板包含副本数/计算资源等可调参数，可允许用户覆盖；未提供则沿用模板默认。
  - 支持显式覆盖：`properties.env` 可用用户输入覆盖模板同名环境变量；`properties.secret` 仅对 `type=secret` 的组件生效，用于覆盖模板的 Secret 数据；`properties.failurePolicy` 仅对 `type=job` 生效，省略时保留模板值、空值时清除模板 override、`cleanup_failed` 时覆盖模板值。
  - init env 覆盖：当 `component.traits.init[].name` 匹配模板 init 容器名时，仅合并 `traits.init[].properties.env`，同名 key 以请求侧为准；若未提供 `name`，默认作用于模板的第一个 init 容器（无 init 则忽略）。
  - 精确匹配：`tmp.target`（模板组件名）用于指定要覆盖的模板组件；未提供时按类型优先匹配，可能存在不确定性。
- 必须重生成：
  - 组件/trait 唯一 ID、内部 UID。
  - 端口号若有冲突需重新分配（保留模板端口偏好，冲突时寻找可用端口）。
- 禁止直接复制：
  - Secret/密码类字段，必须使用占位符或从密钥管理加载。
  - 与运行时绑定的标识（如主机名、PVC 绑定 UID）。
- 默认不重写：
  - RBAC 类特征（ServiceAccount/Role/Binding 名）保持模板定义；命名空间对齐组件命名空间（为空则使用默认命名空间）。

## 标签与审计
- 创建时为应用与组件添加标签/注解：
  - `templateId=<tmp.id>`
  - `templateVersion=<模板版本>`
  - `origin=templated`
  - `createdBy=<user>`、`createdAt=<ts>`
- 审计日志记录模板 ID、请求体、生成的资源名/端口分配。

## 错误与返回码示例
- 404：`tmp.id` 对应模板不存在或不可用。
- 409：命名/端口/配额冲突；重复的幂等请求。
- 400：请求体缺失必填字段或校验失败。
- 500：数据库或下游创建失败（需包含可重试/不可重试标识）。

## 数据库与迁移
- 在 `eruun_applications` 表新增列 `tmp_enable`（bool，默认 false，对应 API 字段 `templateEnabled`），表示该应用是否允许作为模板被引用。
- 应用名称唯一性语义：
  - `templateEnabled=false` 的普通应用要求同命名空间内 `name` 唯一。
  - `templateEnabled=true` 的模板应用要求同命名空间内 `name + version` 唯一，允许同名模板存在不同版本。
  - 模板的 `version` 只参与模板目录身份，不参与运行时 Kubernetes 资源命名。
  - 运行时资源命名：非 shared 组件使用规范化后的 `appName + componentName`；带 `traits.share` 的组件使用 `componentName` 作为命名空间级公共资源 key。
  - 选择器与运行时查询：Pod label、默认 Service selector、日志/容器查询、等待、清理、状态同步统一使用 `AppID + bounded componentName`；原始组件名保存在 annotation 中用于展示和任务聚合。
  - 创建与 `/applications/try` 阶段会提前计算 Deployment/StatefulSet/Service/Ingress/Job/CronJob/ConfigMap/Secret 等独占资源名：普通应用会与同命名空间内其他普通应用做冲突校验；模板应用只校验自身组件是否互相冲突。standalone PVC 只校验名称合法性，允许同命名空间内多个组件或应用共享；`tmpCreate: true` 的 StatefulSet `volumeClaimTemplates` 不视为 standalone PVC。
  - 当前仍处于规则定义阶段，本规则不提供旧资源名迁移路径；已部署资源应按新规则重新创建或清理。
- 迁移要求：
  - 默认值为 false，老数据不自动成为模板。
  - 对现有“官方模板”可通过离线脚本或后台管理界面批量设置 `templateEnabled=true`（落库列 `tmp_enable=true`）。
  - 索引/查询：若模板查询频繁，可在 `tmp_enable` 与 `id` 上建立组合索引以加速过滤。

## 测试要点
- 正常路径：基于模板成功创建应用，组件名称和存储名被正确替换。
- 存储重写：主组件及 init/sidecar 中原始 `name` 相同的 persistent storage 共享一个重写 identity 和 PVC 创建策略；不同名称继续独立重写，顶层非 persistent storage 不参与 persistent identity 匹配。
- 存储创建策略：`tmpCreate=true` 的模板实例化后应生成 volumeClaimTemplate 并挂载同名卷；`tmpCreate=false` 的模板实例化后会按显式 `claimName` 或重写后的 `name` 生成 standalone PVC target，同名 PVC 已存在时部署阶段不更新其 spec。
- 模板默认 StorageClass：`component[].tmp.defaultStorageClass` 会补齐模板内空的 persistent `storageClass`，覆盖顶层 storage 以及 init/sidecar nested storage；已有显式 `storageClass` 和非 persistent storage 保持不变。
- 重复提交：相同 `name` 依赖现有应用创建唯一性约束处理；当前模板实例化请求不提供独立的幂等 token。
- 冲突：端口/名称冲突时返回 409，不产生脏资源；资源名冲突会在创建和 `/applications/try` 阶段提前报错。
- 重复 target：同一次请求多次引用同一个 `tmp.id + target` 时，会为后续克隆组件追加 `-1`、`-2` 等后缀，并按每个 clone 独立重写 Service selector、label、env、Ingress backend 等引用，避免串到第一个 clone。
- 选择器稳定性：即使组件名被规范化或截断，Pod label、默认 Service selector、日志/容器查询、清理和状态同步仍应使用同一个 bounded component label。
- 敏感信息：模板中含 Secret 占位符时，实例化要求用户提供或从配置加载；拒绝直接复制明文。
- 失败语义：中途失败时按现有应用创建链路返回错误；不要在文档中假设额外的模板专属回滚协议。
- 标签追踪：新实例包含来源模板标签/注解。

## 测试示例与验证步骤
- 创建模板应用：调用 `/api/v1/applications` 创建基础模板，设置 `templateEnabled=true`，组件 traits 含存储/Ingress/RBAC 等资源命名，确保模板组件具备镜像与必需字段。
  ```json
  {
    "name": "tmpl-mysql",
    "alias": "mysql-template",
    "version": "1.0.0",
    "project": "demo",
    "description": "mysql base template",
    "component": [
      {
        "name": "mysql",
        "type": "store",
        "image": "mysql:8.0",
        "namespace": "default",
        "replicas": 1,
        "properties": { "ports": [ { "port": 3306, "expose": true } ], "env": { "MYSQL_ROOT_PASSWORD": "__REPLACE_WITH_SECRET__" } },
        "traits": {
          "storage": [ { "name": "mysql", "type": "persistent", "create": true, "size": "5Gi" } ],
          "rbac": [ { "serviceAccount": "mysql", "roleName": "mysql", "bindingName": "mysql" } ]
        }
      }
    ],
    "templateEnabled": true
  }
  ```

- 使用模板默认 StorageClass 创建实例：
  ```json
  {
    "name": "tenant-game-app",
    "namespace": "tenant-a",
    "version": "1.0.0",
    "component": [
      {
        "name": "tenant-mysql",
        "type": "store",
        "tmp": {
          "id": "mysql-template-app-id",
          "target": "mysql",
          "defaultStorageClass": "tenant-a-nas"
        }
      }
    ]
  }
  ```
  模板内空的 persistent storage 会展开为：
  ```json
  {
    "name": "data",
    "type": "persistent",
    "mountPath": "/var/lib/mysql",
    "tmpCreate": true,
    "size": "30Gi",
    "storageClass": "tenant-a-nas"
  }
  ```
  如果模板已显式声明 `"storageClass": "fast-ssd"`，展开后仍保留 `fast-ssd`，不会被 `defaultStorageClass` 覆盖。
- 克隆创建新应用（成功）：请求体中仅提供组件名和 `tmp.id`。期望组件名及存储/Ingress/RBAC/EnvFrom/init/sidecar 中的资源名统一替换为新组件名。
  ```json
  {
    "name": "tenant-a-mysql-app",
    "alias": "tenant-a-mysql",
    "version": "1.0.1",
    "description": "mysql cloned from template",
    "component": [
      { "name": "tenant-a-mysql", "type": "store", "tmp": { "id": "tmpl-mysql-id" } }
    ]
  }
  ```
  验证：调用 `/api/v1/applications/{appID}/components`，检查组件名、traits.storage 的 `name`（以及已声明的 `claimName`/`sourceName`）被重写，Ingress backend 的 `serviceName`、RBAC 的 `serviceAccount/roleName/bindingName` 等按规则替换为新组件名。
- 覆盖规则：如果同一个 `tmp.id` 在请求中出现多次，系统会按 `tmp.target` 或组件类型为每个请求条目生成对应 clone；同一模板组件被重复克隆时，后续目标名会自动追加 `-1`、`-2` 等后缀，支持：
  - 组件重命名：同模板多个条目可为不同组件指定新名称。
  - 环境变量覆盖：`properties.env` 覆盖模板 env。
  - Secret 覆盖：仅 `type=secret` 组件的 `properties.secret` 会覆盖模板 Secret 数据。
  - Job 失败策略覆盖：仅 `type=job` 组件支持 `properties.failurePolicy`；省略保留模板值，空值清除 override，`cleanup_failed` 覆盖模板值，其他值和 init container 使用均拒绝。
  - 精确匹配：推荐在 `tmp` 中提供 `target`（模板组件名），确保覆盖应用到指定组件；缺失 target 时按类型匹配并存在不确定性。
  每个 clone 都会独立生成引用重写映射，确保 Service selector、label、env、Ingress backend 等引用指向自己的 clone。
- 模板未启用错误：当模板 `templateEnabled=false` 时，同样的克隆请求应返回 400，消息 `template application is not enabled`。
- 模板缺失或 ID 为空：`tmp.id` 为空应返回 400（`template id is required`）；不存在的 ID 返回 404（`application not found`）。
- 多组件模板命名：模板包含多个组件（如 `api`、`worker`）时，请求组件名为 `foo-app`，预期实例化后组件名为 `foo-app-api` 与 `foo-app-worker`，并同步重写相关资源名。
- 幂等/冲突：重复提交相同顶层 `name`（或幂等键）应返回已存在；模板组件缺少必需镜像应报 `the image of the component has not been set..`。
- 数据库检查：确认 `eruun_applications.tmp_enable` 新列存在；模板行应为 `true`，克隆出的应用默认为 `false`（除非请求显式设置）。
- 自动化单测参考：运行 `go test ./pkg/apiserver/domain/service -run Template -count=1` 覆盖模板校验与克隆逻辑（需具备写 Go build 缓存权限）。

## 后续演进问题
- 模板升级策略：模板更新后是否影响已实例化的应用（建议默认不回溯，只记录来源版本）。
- 覆盖字段白名单：哪些模板字段允许用户覆盖，是否提供显式的 override 列表。
- 端口分配策略：端口冲突时的自动分配规则与可配置范围。
- 幂等键：仅使用应用 `name` 还是允许显式 `idempotencyKey`。
- 项目/命名空间绑定：顶层 `project` 为空时的默认归属策略。
- 优化提案（待定）：为 `component.tmp` 增加 `overrideMode`（如 `merge|replace|none`）按组件粒度控制合并/替换/禁用覆盖。需明确：
  - 覆盖范围：`merge` 合并 env/secret/config 等 map，`replace` 整体替换这些字段，`none` 仅克隆模板。
  - 匹配规则：同一模板多条目按组件类型或显式 target 匹配，避免重复克隆。
  - 限制校验：合并后对 ConfigMap/Secret 等大小限制做校验，超限时返回错误并提示改用 `replace`，保持行为可预期。
  - 组件定位：考虑在 `component.tmp` 增加 `target`（指向模板组件名/唯一键），请求侧校验 target 存在且类型匹配；若未提供 target，则保留当前类型匹配策略，但需告知顺序不确定或直接报错。
