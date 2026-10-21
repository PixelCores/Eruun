# Try 校验示例

将 JSON 主体（删除 `_comment`、`_expected_errors` 注释字段）作为 `POST /api/v1/applications/try` 主体，`_expected_errors` 记录预期校验错误。Workflow 示例使用文件中标明的 workflow Try 接口；调用需要当前空间的登录身份。

- `16-valid-node-pool.json`：通过 `traits.nodePool` 选择 `gpu-a100`，管理员须先通过 `/api/v1/admin/node-pools` 创建同名节点池。
- `09-invalid-unknown-node-pool.json`：引用不存在的 `unknown-profile` 节点池，返回 `INVALID_TRAIT_CONFIG`，定位 `/components/0/traits/nodePool`。
- `02-valid-complete-app.json`：完整组件示例，节点池引用也需要预先创建。

组件顶层和 command 步骤支持节点池，init/sidecar 不支持。YAML 转换遇到 nodeSelector/tolerations 只提示选择节点池；遇到 Kubernetes RBAC 对象只返回警告。组件不接受 Kubernetes 权限配置字段。
