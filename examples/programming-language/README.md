# Programming Language API 示例

本目录提供管理员维护编程语言选项的 CRUD 示例。`create-dotnet.json` 和 `update-dotnet.json` 是有请求体接口的示例文件；查询列表、查询单条和删除接口没有请求体。

```bash
export ERUUN_API_URL=http://127.0.0.1:8000
```

## 查询列表

```bash
curl -sS "$ERUUN_API_URL/api/v1/programming-languages"
```

响应示例：

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

## 创建

`POST` 不接收 `code`，后端会根据 `name` 生成并在响应中返回。

```bash
curl -sS -X POST "$ERUUN_API_URL/api/v1/programming-languages" \
  -H "Content-Type: application/json" \
  --data @examples/programming-language/create-dotnet.json
```

请求体：

```json
{
  "name": ".NET",
  "version": "8.0",
  "enabled": true,
  "cpuReq": "100m",
  "memReq": "0Mi"
}
```

响应示例：

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

后续查询、更新和删除使用创建响应里的 `data.id`：

```bash
LANG_ID="lang-example-id"
```

## 查询单条

```bash
curl -sS "$ERUUN_API_URL/api/v1/programming-languages/$LANG_ID"
```

响应示例：

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

## 更新

`PUT` 只允许更新 `name`、`version`、`enabled`、`cpuReq`、`memReq`。`code` 创建后不可修改。

```bash
curl -sS -X PUT "$ERUUN_API_URL/api/v1/programming-languages/$LANG_ID" \
  -H "Content-Type: application/json" \
  --data @examples/programming-language/update-dotnet.json
```

请求体：

```json
{
  "name": ".NET",
  "version": "8.0",
  "enabled": false,
  "cpuReq": "200m",
  "memReq": "256Mi"
}
```

响应示例：

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

## 删除

```bash
curl -sS -X DELETE "$ERUUN_API_URL/api/v1/programming-languages/$LANG_ID"
```

响应示例：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "id": "lang-example-id"
  }
}
```
