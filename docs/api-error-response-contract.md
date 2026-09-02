# API 错误响应契约

> 状态：Current。本文说明 API Server 统一响应 envelope 与通用错误脱敏规则。

## 响应 envelope

API Server 使用统一 JSON envelope：

```json
{
  "code": 500,
  "message": "The service has lapsed.",
  "data": null
}
```

- `code`：业务错误码。成功响应为 `0`。
- `message`：面向客户端展示或诊断的错误文案。
- `data`：错误响应通常为 `null`。

客户端应优先依赖 HTTP status 与 `code` 判断错误类型；`message` 适合展示和辅助诊断，不应作为唯一稳定解析键。

## 错误映射

由统一错误入口处理的错误遵循以下规则：

| 错误来源 | HTTP status | `code` | `message` |
| --- | --- | --- | --- |
| 业务错误 `*Bcode` | 业务错误定义值 | 业务错误定义值 | 业务错误定义文案 |
| 显式标记为客户端安全的业务错误 | 对应业务错误定义值 | 对应业务错误定义值 | 经该业务路径审查的客户端提示 |
| validator 校验错误 | `400` | `10000` | `application config does not comply with OAM specification` |
| `datastore.ErrRecordNotExist` | `404` | `404` | `404 Not Found` |
| 非业务通用错误 | `500` | `500` | `The service has lapsed.` |

应用不存在是业务级不存在，响应使用 HTTP `200`，body 中返回 `code=10005`、`message=application not found`；客户端应依赖 body `code` 区分该场景。

非业务通用错误不会在响应体中回显原始 `err.Error()`。这包括但不限于 DB、Kubernetes、网络、文件路径、DSN、password、token、API key 等内部或敏感细节。

## 日志行为

非业务通用错误会写入服务端日志用于定位，但日志只保留稳定定位信息和错误类型，不记录原始错误文本。这样可以避免把同一个敏感信息从 API 响应体转移到运维日志中。

显式调用自定义错误文案，或在业务错误链上显式标记客户端安全文案的接口，仍可能返回该接口定义的业务提示；这类文案应由对应 API 文档说明。安全文案只携带客户端采取行动所需的信息，原始内部错误仍保留在服务端错误链中，不得把 before/after 规格、凭证或基础设施细节带入响应。
