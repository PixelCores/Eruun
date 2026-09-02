# Programming Language API

> 状态：Current。本文说明管理员维护编程语言选项的 REST API。该能力只管理语言配置，不参与应用创建、模板克隆或 Workflow 执行校验。

## 字段语义

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | string | 后端生成的记录 ID。 |
| `code` | string | 后端根据 `name` 机械生成的稳定机器标识，只在响应中输出，创建后不可修改。只做通用 slug/符号编码，不做语言别名归并；字母和数字保留，普通分隔符折叠为 `-`，其他符号使用十六进制 token 避免 `C`、`C#`、`C++` 冲突。只允许小写字母、数字和中划线，长度 1-64。 |
| `name` | string | 展示名称，例如 `.NET`、`Golang`。 |
| `version` | string | 语言版本，例如 `8.0`、`1.24`。同一个 `code + version` 只能存在一条记录。 |
| `enabled` | bool | 是否启用。当前只作为管理状态返回，不影响现有 Workflow 或应用接口。 |
| `cpuReq` | string | CPU request，使用 Kubernetes quantity，例如 `100m`、`1`。 |
| `memReq` | string | Memory request，使用 Kubernetes quantity，例如 `0Mi`、`512Mi`。 |
| `createTime` / `updateTime` | string | 记录创建和更新时间。 |

## List

```bash
curl -sS "$ERUUN_API_URL/api/v1/programming-languages"
```

响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "languages": [
      {
        "id": "lang-example-id",
        "code": "net",
        "name": ".NET",
        "version": "8.0",
        "enabled": true,
        "cpuReq": "100m",
        "memReq": "0Mi",
        "createTime": "2026-06-17T10:00:00Z",
        "updateTime": "2026-06-17T10:00:00Z"
      }
    ]
  }
}
```

## Get

```bash
curl -sS "$ERUUN_API_URL/api/v1/programming-languages/{id}"
```

响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "id": "lang-example-id",
    "code": "net",
    "name": ".NET",
    "version": "8.0",
    "enabled": true,
    "cpuReq": "100m",
    "memReq": "0Mi",
    "createTime": "2026-06-17T10:00:00Z",
    "updateTime": "2026-06-17T10:00:00Z"
  }
}
```

## Create

`POST` 不接收 `code`，后端会根据 `name` 做机械 slug/符号编码并在响应中输出。传入 `code` 会被 strict JSON 校验拒绝。

```bash
curl -sS -X POST "$ERUUN_API_URL/api/v1/programming-languages" \
  -H "Content-Type: application/json" \
  --data @examples/programming-language/create-dotnet.json
```

请求：

```json
{
  "name": ".NET",
  "version": "8.0",
  "enabled": true,
  "cpuReq": "100m",
  "memReq": "0Mi"
}
```

响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "id": "lang-example-id",
    "code": "net",
    "name": ".NET",
    "version": "8.0",
    "enabled": true,
    "cpuReq": "100m",
    "memReq": "0Mi",
    "createTime": "2026-06-17T10:00:00Z",
    "updateTime": "2026-06-17T10:00:00Z"
  }
}
```

## Update

`PUT` 只允许更新 `name`、`version`、`enabled`、`cpuReq`、`memReq`。`code` 创建后不可修改，传入 `code` 会被 strict JSON 校验拒绝。

```bash
curl -sS -X PUT "$ERUUN_API_URL/api/v1/programming-languages/{id}" \
  -H "Content-Type: application/json" \
  --data @examples/programming-language/update-dotnet.json
```

请求：

```json
{
  "name": ".NET",
  "version": "8.0",
  "enabled": false,
  "cpuReq": "200m",
  "memReq": "256Mi"
}
```

响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "id": "lang-example-id",
    "code": "net",
    "name": ".NET",
    "version": "8.0",
    "enabled": false,
    "cpuReq": "200m",
    "memReq": "256Mi",
    "createTime": "2026-06-17T10:00:00Z",
    "updateTime": "2026-06-17T10:05:00Z"
  }
}
```

## Delete

```bash
curl -sS -X DELETE "$ERUUN_API_URL/api/v1/programming-languages/{id}"
```

响应：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "id": "lang-example-id"
  }
}
```

## 错误码

| HTTP | code | message | 场景 |
| --- | --- | --- | --- |
| 400 | 32000 | `programming language is invalid` | 字段缺失、格式非法、无法从 `name` 生成合法 `code`、请求传入不允许的字段、资源规格不是 Kubernetes quantity。 |
| 409 | 32001 | `programming language already exists` | `code + version` 已存在。 |
| 404 | 32002 | `programming language not found` | 按 `id` 查询、更新或删除的记录不存在。 |
