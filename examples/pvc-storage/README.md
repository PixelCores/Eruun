# PVC Storage 测试用例

本目录包含用于测试 StatefulSet PVC Volume 命名问题的测试用例。

## 背景

在 StatefulSet 中使用 `tmpCreate: true` 创建持久化存储时，volumeClaimTemplate 的名称必须与 VolumeMount 的名称一致，否则会导致 Pod 创建失败。

详细的问题分析请参阅：[StatefulSet PVC Volume 命名问题深度分析](../../docs/statefulset-pvc-volume-naming.md)

## 测试用例列表

| 文件 | 场景 | 目的 |
|------|------|------|
| `01-basic-statefulset-pvc.json` | 基本 StatefulSet + tmpCreate | 验证单个 PVC 的正确创建 |
| `02-multi-volume-statefulset.json` | 多 Volume StatefulSet | 验证多个 tmpCreate volume 同时工作 |
| `03-mixed-pvc-mode.json` | 混合模式 | 验证 tmpCreate + 引用已有 PVC |
| `04-multi-container-shared-volume.json` | 多容器共享 Volume | 验证主容器 + Sidecar + Init 共享 volume |
| `05-dependency-chain-with-pvc.json` | 依赖链 | 验证复杂工作流中的 PVC 处理 |
| `06-deployment-ephemeral-only.json` | Deployment + ephemeral | Deployment 推荐的存储方式 |

## 使用方法

### 1. 启动 API Server

```shell
export ERUUN_DATASTORE_URL='root:123456@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true'
go run ./cmd/main.go
```

默认 API Server 监听 `127.0.0.1:8000`；如果通过集群 manifest 部署并 port-forward 到本地 8080，可把 `ERUUN_API_URL` 改成对应地址。

### 2. 执行测试

```shell
export ERUUN_API_URL=http://127.0.0.1:8000
export API_ADDR="${ERUUN_API_URL}/api/v1"
export TEST_FILE=examples/pvc-storage/01-basic-statefulset-pvc.json

# 可选：先做 dry-run 校验；该接口只返回校验结果，不会创建应用、任务或 Kubernetes 资源
curl -fsS -X POST "${API_ADDR}/applications/try" \
  -H "Content-Type: application/json" \
  -d @"${TEST_FILE}"

# 创建应用
APP_RESP="$(curl -fsS -X POST "${API_ADDR}/applications" \
  -H "Content-Type: application/json" \
  -d @"${TEST_FILE}")"
APP_ID="$(printf '%s' "${APP_RESP}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])')"
WORKFLOW_ID="$(printf '%s' "${APP_RESP}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["workflowId"])')"

# 执行创建返回的默认 workflow，并记录任务 ID
EXEC_RESP="$(curl -fsS -X POST "${API_ADDR}/applications/${APP_ID}/workflow/exec" \
  -H "Content-Type: application/json" \
  -d "{\"workflowId\":\"${WORKFLOW_ID}\"}")"
TASK_ID="$(printf '%s' "${EXEC_RESP}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["taskId"])')"

# 查询 workflow 任务状态
curl -fsS "${API_ADDR}/workflow/tasks/${TASK_ID}/status"
```

### 3. 验证 Kubernetes 资源

```shell
export KUBECTL_NAMESPACE=default
export COMPONENT_LABEL=eruun.io/component-name=mysql-primary

# 检查 StatefulSet volumeClaimTemplates，应包含 mysql-data
kubectl -n "${KUBECTL_NAMESPACE}" get sts -l "${COMPONENT_LABEL}" \
  -o jsonpath='{range .items[*]}{.metadata.name}: {.spec.volumeClaimTemplates[*].metadata.name}{"\n"}{end}'

# 检查 Pod volumeMounts，应包含 mysql-data
kubectl -n "${KUBECTL_NAMESPACE}" get pod -l "${COMPONENT_LABEL}" \
  -o jsonpath='{range .items[*]}{.metadata.name}: {.spec.containers[*].volumeMounts[*].name}{"\n"}{end}'

# 检查 StatefulSet 自动创建的 PVC，名称形如 mysql-data-<statefulset-pod-name>
kubectl -n "${KUBECTL_NAMESPACE}" get pvc | grep '^mysql-data-'
```

## 预期结果

### 正确的 StatefulSet YAML 结构

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql-primary-xxx
spec:
  volumeClaimTemplates:
    - metadata:
        name: mysql-data  # ✅ 与 VolumeMount 名称一致
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 5Gi
  template:
    spec:
      containers:
        - name: mysql-primary
          volumeMounts:
            - name: mysql-data  # ✅ 匹配 volumeClaimTemplate.name
              mountPath: /var/lib/mysql
```

### 常见错误

如果看到以下错误，说明 PVC 命名逻辑存在问题：

```
spec.containers[0].volumeMounts[0].name: Not found: "mysql-data"
```

## 注意事项

1. **tmpCreate 仅适用于 StatefulSet**：Deployment 不支持 volumeClaimTemplates，使用 tmpCreate 会导致警告
2. **Volume 名称去重**：多个容器引用同一个 volume 时，只会创建一个 volumeClaimTemplate
3. **subPath 支持**：可以使用 subPath 在同一个 PVC 上隔离不同容器的数据
4. **显式 claimName 指定 standalone PVC 名**：PVC 不存在时 Eruun 会创建；同名 PVC 已存在时 Eruun 不更新容量、StorageClass、AccessModes 或其他 spec

## 相关文档

- [StatefulSet PVC Volume 命名问题深度分析](../../docs/statefulset-pvc-volume-naming.md)
- [Workflow 测试指南](../../docs/workflow-testing-guide.md)
