# 组件容器信息查询示例

该目录包含组件容器信息查询接口的调用与响应示例。

## 查询 Pod 型组件容器信息

```bash
curl -sS \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/api/containers"
```

参考响应：`examples/component-containers/list-component-containers-response.json`

## 查询非 Pod 型组件（如 config）

```bash
curl -sS \
  "$ERUUN_API_URL/api/v1/applications/app-1/components/config-center/containers"
```

参考响应：`examples/component-containers/list-component-containers-empty-response.json`
