# 出站 URL 安全策略（urlSecurityPolicy）

> 状态：Current。当前工作流回调、远程配置拉取与 convert fileURL 会通过该策略收敛出站 URL 校验。

## 背景与目标
- 目标：统一收敛 Eruun 的出站 URL 安全校验，降低 SSRF 风险。
- 覆盖链路：
  - 应用工作流回调 URL 校验
  - Workflow Callback Job 请求发送
  - ConfigMap/Secret 远程 URL 拉取
  - `/applications/convert` 的 `fileURL` 拉取
- 关键改进：
  - 私网目标不再仅告警，默认阻断
  - 重定向逐跳校验，防止“首跳公网、次跳私网”绕过
  - 策略统一从 `system_setting.type=urlSecurityPolicy` 加载

## 策略模型
`system_setting.type = "urlSecurityPolicy"`，`value` 为 JSON 对象：

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

字段说明：
- `allowPrivateByDefault`: 是否默认允许私网/回环/链路本地目标
- `allowedHostPatterns`: 主机白名单
  - 支持精确主机：`api.internal.example.com`
  - 支持 `*.` 前缀通配：`*.svc.cluster.local`
- `allowedCIDRs`: 私网 CIDR 白名单（如 `10.0.0.0/8`）

## 生效优先级
- 优先读取 `system_setting(urlSecurityPolicy)`。
- 运行期如果 provider 未注入、配置缺失、读取失败、JSON 解析失败或策略校验失败，相关出站 URL 操作会直接失败，不再回退到 legacy/default policy。
- 历史开关 `allow-private-url-targets` / `ERUUN_ALLOW_PRIVATE_URL_TARGETS` 仅用于启动期 bootstrap 默认 `urlSecurityPolicy`，不参与运行期兜底。
- 启动期会自动 bootstrap 一条默认 `urlSecurityPolicy`，保证新环境可直接生效；若 bootstrap 后配置被删除或损坏，运行期按配置不可用处理。

## 重定向安全规则
- 首次请求前先校验 URL 目标。
- 遇到 3xx 跳转时，每一跳都再次按同一策略校验 `Location`。
- 任意一跳命中“非白名单私网目标”则请求失败。

## 实际连接与代理规则
- 远程拉取和 Workflow Callback 的实际 `DialContext` 会解析目标主机、按本策略校验解析出的全部 IP，并直接拨号到同一批已校验 IP；HTTP `Host` 和 HTTPS SNI 仍使用原始 URL 主机名。校验与实际连接不再由两个独立 DNS 结果决定。
- 多 A/AAAA 地址按有界 Happy Eyeballs 语义拨号：首个地址族优先，另一个地址族延迟 300ms 回退；策略 Transport 会保留请求的绝对截止时间，DNS 解析和同一地址族的候选拨号共同受请求剩余时限与 30 秒拨号上限约束，避免单个不可达地址耗尽整次请求。
- IPv6 解析结果会保留 scoped address 的 zone；允许访问的链路本地地址会携带原 zone 拨号。zone 不参与 CIDR 判断，也不会绕过默认私网阻断规则。
- 远程拉取和 Workflow Callback 的一次性策略客户端会在操作结束后关闭空闲连接，不跨策略保留或复用连接池。
- 受 `urlSecurityPolicy` 保护的请求不使用 `HTTP_PROXY`、`HTTPS_PROXY` 或自定义 Transport proxy；显式基础 `*http.Transport` 若配置 `DialContext`、`Dial`、`DialTLSContext`、`DialTLS` 或调用方自定义的 `TLSNextProto` 会 fail closed，显式或隐式基础 Transport 设置非空 `TLSClientConfig.ServerName` 也会 fail closed。标准库惰性生成的 HTTP/2 protocol hook 会在克隆后安全重建，不视为调用方 hook；同一基础 Transport 可重复或并发构造策略客户端，并保持原有 HTTP/1.1、HTTP/2 协商资格。当前策略模型没有受信代理、拨号 hook 或 TLS 身份覆盖名单，因而这些能力不能绕过已验证 IP 与原始 URL Host/SNI 契约；无法解析时 fail closed。
- 如未来需要代理出站，必须先扩展独立的受信代理配置并同时约束代理地址与代理端目标解析，不能恢复 unresolved host 默认放行。

## 默认值与初始化
- 默认允许模式：`allowPrivateByDefault=false`
- 默认主机白名单：
  - `*.svc.cluster.local`
  - `*.paas.example.com`（占位示例，上线前请替换为真实 PaaS 域名）

可用初始化脚本：
- `scripts/init-system-setting.sql`

执行示例：
```bash
mysql -h <host> -u <user> -p <database> < scripts/init-system-setting.sql
```

## API 管理示例
使用已有系统设置接口即可，无需新增路由。

创建：
```bash
curl -sS -X POST "http://127.0.0.1:8000/api/v1/settings" \
  -H "Content-Type: application/json" \
  --data @examples/system-setting/create-url-security-policy-setting.json
```

查询：
```bash
curl -sS "http://127.0.0.1:8000/api/v1/settings/urlSecurityPolicy"
```

更新：
```bash
curl -sS -X PUT "http://127.0.0.1:8000/api/v1/settings/urlSecurityPolicy" \
  -H "Content-Type: application/json" \
  --data '{
    "value": {
      "allowPrivateByDefault": false,
      "allowedHostPatterns": ["*.svc.cluster.local", "*.corp-paas.example.com"],
      "allowedCIDRs": ["10.0.0.0/8"]
    }
  }'
```

## 运维建议
- 生产环境建议保持 `allowPrivateByDefault=false`。
- 将 `urlSecurityPolicy` 视为必需运行配置；迁移、初始化或配置中心变更后应确认该记录存在且 JSON 合法。
- `*.paas.example.com` 仅为占位示例，必须替换为真实域名后再上线。
- 不建议使用过宽 CIDR（如 `0.0.0.0/0`），避免白名单失效。
- 变更白名单后建议做一次回调与远程拉取验证（含重定向场景）。
