# 批量应用详情查询 API

> 状态：Current。当前路由为 `POST /api/v1/applications/query`。

本文档描述按多个应用 ID 批量查询应用基础信息和组件摘要信息的接口。

## 接口

- 方法：`POST`
- 路径：`/api/v1/applications/query`
- 用途：传入多个 `appId`，按请求顺序返回对应应用基础信息及组件摘要。

## 请求体

```json
{
  "appIds": ["app-1", "app-2"]
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `appIds` | `string[]` | 是 | 应用 ID 列表。列表不能为空，元素不能为空字符串。 |

## 响应体

```json
{
  "applications": [
    {
      "id": "app-1",
      "name": "demo",
      "namespace": "default",
      "alias": "",
      "project": "demo-project",
      "version": "1.0.0",
      "description": "",
      "createTime": "2026-04-30T00:00:00Z",
      "updateTime": "2026-04-30T00:00:00Z",
      "icon": "",
      "workflowId": "workflow-1",
      "templateEnabled": false,
      "resources": {
        "cpuReq": "",
        "memReq": "",
        "cpuLimit": "",
        "memLimit": "",
        "replicas": 0
      },
      "components": [
        {
          "id": 1,
          "appId": "app-1",
          "name": "backend",
          "namespace": "default",
          "replicas": 1,
          "type": "webservice",
          "properties": {
            "ports": [
              {
                "port": 8080
              }
            ]
          }
        }
      ]
    }
  ]
}
```

响应说明：

- 应用基础字段复用 `ApplicationBase`，其中资源摘要位于 `resources`。
- `components` 返回轻量组件摘要，包含 `id`、`appId`、`name`、`namespace`、`replicas`、`type`、`properties`。
- `components[].properties` 仅返回 `ports`，端口结构为 `{ "port": number }`。
- `components` 不返回 `properties.env`、`properties.conf`、`properties.secret`、`properties.command`、`properties.labels`、`traits`、`sidecars`、`services`、`resourceConfigs`、`credentials`、`externalLinks` 等详情字段，避免批量查询响应过大或暴露敏感配置。
- 返回顺序与请求 `appIds` 顺序一致。
- 如果请求里重复传入同一个 `appId`，响应也会按请求顺序重复返回。
- 如果任一 `appId` 不存在，接口整体返回应用不存在错误，错误消息为 `application not found`。

## 示例

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/applications/query" \
  -H "Content-Type: application/json" \
  -d '{"appIds":["app-1","app-2"]}'
```
