# 系统设置（System Setting）

> 状态：Current。本文按当前 `system_setting` 模型、设置校验和已注册路由整理。

## 变更摘要
- 新增统一的系统设置表 `eruun_system_setting`，用 `type` 区分配置类型，用 `value` 存放 JSON/JSON 数组。
- 废弃旧表：`eruun_node_selector_profiles`、`eruun_rbac_profiles`（启动时自动迁移并删除）。
- 新增 `aliyunCloud` 设置类型，用于承载 aliyun cloudjob 的凭证与默认拓扑信息。
- 新增 `aliyunCloud` 保存前连通性校验：创建或更新时会先调用阿里云 NAS 只读查询接口，校验通过后才允许落库。
- 新增 `urlSecurityPolicy` 出站 URL 安全策略，支持私网白名单控制与重定向逐跳校验。
- 新增 `podRestartMonitor` Pod 重启监控策略，用于配置 Deployment 下单个 Pod 在滑动时间窗口内的重启触发阈值。
- 当前 `master` 临时不暴露 OAuth 与 authz 管理路由：`/api/v1/auth/oauth2/google/*`、`/api/v1/authz/*`（访问将返回 `404`）。

## 数据结构
- `type`: 目前支持 `nodeSelector`、`rbacPolicies`、`apiAuth`、`oauthAuth`、`aliyunCloud`、`urlSecurityPolicy`、`podRestartMonitor`
- `value`: JSON 或 JSON 数组
  - `nodeSelector`: 建议为 JSON 对象或数组，结构与 `NodeSelectionSpec`（`nodeSelector/affinity/tolerations`）一致，或包含历史迁移的 profile 列表
  - `rbacPolicies`: 必须为 JSON 数组，每个元素为 RBAC Policy 配置
  - `apiAuth`: 必须为 JSON 对象，定义 API JWT 鉴权与路由级 RBAC 授权策略
    - 安全约束：`GET /settings` 与 `GET /settings/{type}` 的返回会对 `jwt.hs256.secret` 做掩码处理（`******`）
    - 更新约束：写入接口不接受 `jwt.hs256.secret = "******"`（该值仅用于读取脱敏展示）
    - 生效约束：成功写入后，所有副本的后续非跳过路径请求立即读取并执行新策略；不使用本地 TTL 缓存
  - `oauthAuth`: 必须为 JSON 对象，定义 Google OAuth2 登录参数、本地 JWT 签发参数与角色映射规则
    - 安全约束：`GET /settings` 与 `GET /settings/{type}` 的返回会对 `providers.google.clientSecret` 做掩码处理（`******`）
    - 更新约束：写入接口不接受 `providers.google.clientSecret = "******"`（该值仅用于读取脱敏展示）
  - `aliyunCloud`: 必须为 JSON 对象，定义 aliyun cloudjob 的凭证与默认 region / zone / vpc / vsw 信息
    - 规范字段：`accessKeyId`、`accessKeySecret`、`endpoint`、`regionId`、`zoneId`、`vpcId`、`vswId`
    - 兼容写入别名：`access_key_id`、`access_key_secret`、`region_id`、`zone_id`、`vpc_id`、`vsw_id`
    - 安全约束：`GET /settings` 与 `GET /settings/{type}` 的返回会对 `accessKeySecret` 做掩码处理（`******`）
    - 更新约束：写入接口不接受 `accessKeySecret = "******"`，也不接受 `region_endpoint`
    - 冲突约束：若 camelCase 字段与 snake_case 别名同时出现且值不同，接口会直接报错
    - 连通性约束：`POST /settings` 与 `PUT /settings/{type}` 保存 `aliyunCloud` 时，会先调用阿里云 NAS `DescribeFileSystems` 只读接口验证配置可用；AK/SK、region、endpoint 错误，或账号缺少 NAS 只读权限时会直接返回失败且不落库
  - `urlSecurityPolicy`: 必须为 JSON 对象，定义出站 URL 私网访问策略
    - 关键字段：`allowPrivateByDefault`、`allowedHostPatterns`、`allowedCIDRs`
    - `allowedHostPatterns` 支持精确主机与 `*.` 通配后缀（如 `*.svc.cluster.local`）
    - 详细说明见 `docs/url-security-policy.md`
  - `podRestartMonitor`: 必须为 JSON 对象，定义 Pod 重启阈值触发策略
    - 关键字段：`enabled`、`windowSeconds`、`threshold`
    - 默认值：`enabled=true`、`windowSeconds=1800`、`threshold=3`
    - 行为：Deployment 管理的单个 Pod 在窗口内累计重启次数达到阈值时触发占位处理函数；同一窗口内只触发一次
    - `windowSeconds` 与 `threshold` 必须大于 0

`apiAuth` 示例（中间件已接入全局路由；默认初始化为 `enabled=false`，启用后才开始鉴权/授权）：
```json
{
  "enabled": true,
  "jwt": {
    "algorithms": ["HS256", "RS256"],
    "issuers": ["eruun"],
    "audience": ["eruun-api"],
    "clockSkewSeconds": 60,
    "hs256": {
      "secret": "replace-with-secret"
    },
    "rs256": {
      "publicKeys": [
        {
          "kid": "key-1",
          "pem": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----"
        }
      ]
    }
  },
  "authorization": {
    "defaultEffect": "deny",
    "routes": [
      {
        "method": "GET",
        "path": "/api/v1/applications",
        "roles": ["admin", "reader"]
      }
    ]
  }
}
```

`oauthAuth` 示例（Google OAuth2 + 本地 JWT 签发）：
```json
{
  "enabled": true,
  "providers": {
    "google": {
      "clientId": "replace-with-google-client-id",
      "clientSecret": "replace-with-google-client-secret",
      "redirectURI": "https://your-eruun.example.com/api/v1/auth/oauth2/google/callback",
      "scopes": ["openid", "email", "profile"]
    }
  },
  "jwtIssue": {
    "issuer": "eruun",
    "audience": "eruun-api",
    "ttlSeconds": 3600
  },
  "roleMapping": {
    "defaultRoles": ["reader"],
    "googleHostedDomainToRoles": {
      "example.com": ["admin"]
    },
    "googleEmailToRoles": {
      "owner@example.com": ["admin"]
    }
  },
  "security": {
    "stateTTLSeconds": 300
  }
}
```

`aliyunCloud` 示例：
```json
{
  "accessKeyId": "replace-with-access-key-id",
  "accessKeySecret": "replace-with-access-key-secret",
  "endpoint": "nas.cn-hangzhou.aliyuncs.com",
  "regionId": "cn-hangzhou",
  "zoneId": "cn-hangzhou-i",
  "vpcId": "vpc-xxxx",
  "vswId": "vsw-xxxx"
}
```

`urlSecurityPolicy` 示例（默认最小可用白名单）：
```json
{
  "allowPrivateByDefault": false,
  "allowedHostPatterns": [
    "*.svc.cluster.local",
    "*.paas.example.com"
  ],
  "allowedCIDRs": []
}
```

`podRestartMonitor` 示例：
```json
{
  "enabled": true,
  "windowSeconds": 1800,
  "threshold": 3
}
```

## API
### 创建 Setting
- `POST /api/v1/settings`

请求示例：
```json
{
  "type": "nodeSelector",
  "value": {
    "nodeSelector": {
      "node.kubernetes.io/test": "on"
    }
  }
}
```

### 更新 Setting
- `PUT /api/v1/settings/{type}`

请求示例：
```json
{
  "value": [
    {
      "serviceAccount": "default",
      "rules": [
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}
      ]
    }
  ]
}
```

### 查询单条 Setting
- `GET /api/v1/settings/{type}`

### 查询全部 Setting
- `GET /api/v1/settings`

### 删除 Setting
- `DELETE /api/v1/settings/{type}`

### 查询 API 授权策略
- `GET /api/v1/authz/routes`

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

返回 `apiAuth.authorization` 的可执行策略视图，包含：
- `defaultEffect`: 默认放行/拒绝策略（`allow` / `deny`）
- `routes`: 路由级角色规则（`method` + `path` + `roles`）

### 新增或更新一条授权路由规则
- `PUT /api/v1/authz/routes`

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

请求示例：
```json
{
  "method": "POST",
  "path": "/api/v1/applications",
  "roles": ["admin", "deployer"]
}
```

行为说明：
- 若 `method + path` 已存在，则更新该规则的 `roles`
- 若不存在，则追加新规则
- 仅修改 `authorization`，不会覆盖已配置的 JWT 密钥信息

### 删除一条授权路由规则
- `DELETE /api/v1/authz/routes?method={METHOD}&path={PATH}`

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

示例：
- `DELETE /api/v1/authz/routes?method=POST&path=/api/v1/applications`

### 更新默认授权策略
- `PATCH /api/v1/authz/default-effect`

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

请求示例：
```json
{
  "defaultEffect": "deny"
}
```

## 默认初始化脚本
- 脚本路径：`scripts/init-system-setting.sql`
- 作用：初始化 `nodeSelector`、`rbacPolicies`、`apiAuth`、`oauthAuth`、`urlSecurityPolicy` 与 `podRestartMonitor` 默认记录（幂等、非破坏性）。
- 说明：`aliyunCloud` 不会自动初始化，因为它需要真实的云账号与网络拓扑信息。
- 执行示例：

```bash
mysql -h <host> -u <user> -p <database> < scripts/init-system-setting.sql
```

### curl 示例
以下示例默认服务地址为 `http://127.0.0.1:8000`：

```bash
SERVER="http://127.0.0.1:8000"
```

创建 `apiAuth`（使用示例文件）：

```bash
curl -sS -X POST "${SERVER}/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-api-auth-setting.json
```

创建 `oauthAuth`（使用示例文件）：

```bash
curl -sS -X POST "${SERVER}/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-oauth-auth-setting.json
```

创建 `aliyunCloud`（使用示例文件）：

```bash
curl -sS -X POST "${SERVER}/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-aliyun-cloud-setting.json
```

说明：
- 该请求会在保存前主动校验阿里云 NAS 连通性。
- 若返回失败，请优先检查 `accessKeyId`、`accessKeySecret`、`regionId`、`endpoint`，以及当前账号是否具备 NAS 只读查询权限。

创建 `urlSecurityPolicy`（使用示例文件）：

```bash
curl -sS -X POST "${SERVER}/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-url-security-policy-setting.json
```

创建 `podRestartMonitor`（使用示例文件）：

```bash
curl -sS -X POST "${SERVER}/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-pod-restart-monitor-setting.json
```

查询单条 `urlSecurityPolicy`：

```bash
curl -sS "${SERVER}/api/v1/settings/urlSecurityPolicy"
```

查询单条 `podRestartMonitor`：

```bash
curl -sS "${SERVER}/api/v1/settings/podRestartMonitor"
```

查询单条 `aliyunCloud`（注意返回中的 `accessKeySecret` 为脱敏值 `******`）：

```bash
curl -sS "${SERVER}/api/v1/settings/aliyunCloud"
```

更新 `apiAuth`（示例：调整默认策略）：

```bash
curl -sS -X PUT "${SERVER}/api/v1/settings/apiAuth" \
  -H "Content-Type: application/json" \
  --data '{
    "value": {
      "enabled": true,
      "jwt": {
        "algorithms": ["HS256"],
        "hs256": {
          "secret": "replace-with-new-secret"
        }
      },
      "authorization": {
        "defaultEffect": "deny",
        "routes": [
          {
            "method": "GET",
            "path": "/api/v1/applications",
            "roles": ["admin", "reader"]
          }
        ]
      }
    }
  }'
```

查询单条（注意返回中的 `jwt.hs256.secret` 为脱敏值 `******`）：

```bash
curl -sS "${SERVER}/api/v1/settings/apiAuth"
```

查询单条 `oauthAuth`（注意返回中的 `providers.google.clientSecret` 为脱敏值 `******`）：

```bash
curl -sS "${SERVER}/api/v1/settings/oauthAuth"
```

查询全部：

```bash
curl -sS "${SERVER}/api/v1/settings"
```

删除：

```bash
curl -sS -X DELETE "${SERVER}/api/v1/settings/apiAuth"
```

Google OAuth2 登录入口（302 跳转至 Google 授权页）：

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

```bash
curl -i "${SERVER}/api/v1/auth/oauth2/google/login"
```

Google OAuth2 回调示例：

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

```bash
curl -sS "${SERVER}/api/v1/auth/oauth2/google/callback?code=<code>&state=<state>"
```

查询当前授权策略：

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

```bash
curl -sS "${SERVER}/api/v1/authz/routes"
```

新增/更新一条授权路由规则（使用示例文件）：

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

```bash
curl -sS -X PUT "${SERVER}/api/v1/authz/routes" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/upsert-api-authz-route.json
```

删除一条授权路由规则：

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

```bash
curl -sS -X DELETE "${SERVER}/api/v1/authz/routes?method=POST&path=/api/v1/applications"
```

更新默认授权策略（使用示例文件）：

> 注意：当前 `master` 该路由处于临时停用状态（未注册，访问返回 `404`）。

```bash
curl -sS -X PATCH "${SERVER}/api/v1/authz/default-effect" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/update-api-authz-default-effect.json
```

## 错误码
- `30000`: system setting type is invalid
- `30001`: system setting value is invalid
- `30002`: system setting already exists
- `30003`: system setting not found

## 迁移说明
服务启动时将：
1. 读取 `eruun_node_selector_profiles` 与 `eruun_rbac_profiles`
2. 将内容写入 `eruun_system_setting`（分别为 `nodeSelector` / `rbacPolicies`）
3. 删除旧表

## 测试
- `go test ./pkg/apiserver/domain/service -run TestSystemSettingService`
