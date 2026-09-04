# 模板同名持久化存储 Identity 实现说明

> 状态：Implemented Reference。本文记录 `main` 已实现的模板持久化存储 Identity 规则及回归验证；公开模板契约以 `template-instantiation-from-tem-id.md` 和代码为准。

## 问题与边界

调用方可以在模板实例化请求中传入 `component[].tmp.defaultStorageClass`。Eruun 的职责是把该值应用到模板展开后的 persistent storage，并保证同一逻辑存储只生成一个 PVC。

原问题发生在 MySQL 模板同时定义以下存储时：

- 顶层 `traits.storage[].name=data`，`tmpCreate=true`，应生成 StatefulSet `volumeClaimTemplate`；
- init 或 sidecar 中也使用原始名称 `data` 挂载同一数据目录，但未声明或未保留 `tmpCreate=true`。

若模板实例化把嵌套 storage 当作独立存储重写，它会落入 `tmpCreate=false` 的 standalone PVC 路径。结果是一个 `volumeClaimTemplate` PVC 加一个额外 standalone PVC。

当前实现只修改 Eruun 的模板克隆语义；不修改调用方请求字段、不迁移既有 PVC，也不改变 `claimName` 独立引用的既有契约。

## 当前契约

同一模板组件中，顶层 storage 与 init/sidecar storage 同时满足以下条件时，视为同一个逻辑存储：

1. 两者都是 `type=persistent`；
2. 两者的原始 `name` 相同（比较前 `TrimSpace`）；
3. 顶层 storage 是该 logical storage 的声明源。

嵌套 storage 必须继承顶层 persistent storage 完成模板重写后的 `name`、`tmpCreate`、`claimName`、`size`、`storageClass`。嵌套项只保留自身的 `mountPath`、`subPath`、`subPathExpr`、`readOnly`，以便不同容器按各自路径挂载同一个 volume；同一路径在不同容器中合法，但每个容器只能得到一个对应的 `VolumeMount`。

名称不同的 persistent storage 继续沿用当前独立重写规则；显式 `claimName` 的 standalone PVC 仍由原有 `claimName` 语义处理。

## 实现路径

1. 保持调用方传入的 `tmp.defaultStorageClass` 接口不变。
2. `pkg/apiserver/domain/service/application/application_template_clone_traits.go` 通过内部递归函数传递以原始 storage 名为 key 的 identity 映射。
3. 顶层 `traits.storage` 完成既有名称与 `claimName` 重写后，只将非空原始名称对应的 persistent storage 记录进 identity 映射；顶层非 persistent storage 不参与匹配。
4. 递归处理 `traits.init[].traits` 与 `traits.sidecar[].traits` 时传入该映射：命中同名 persistent storage 后仅继承 PVC identity 字段，并保留该嵌套项的挂载相关字段。
5. 保持 `StorageProcessor`、StatefulSet `volumeClaimTemplates` 转换、standalone PVC 创建与 `job_pvc.go` 不变。修复应在模板克隆阶段消除错误的独立 storage，而不是在 PVC Job 阶段猜测或删除 PVC。
6. 同步 `docs/template-instantiation-from-tem-id.md` 的持久化存储契约，明确同名 init/sidecar storage 与顶层 storage 共享 identity。

## 回归测试

在 `pkg/apiserver/domain/service/application/application_template_clone_test.go` 覆盖真实模板克隆到 StatefulSet 渲染：

- 顶层 `data` 为 `tmpCreate=true`，init/sidecar 的原始 `data` 不声明 `tmpCreate`；
- 三个容器都只挂载一次 `data`，嵌套项保留自己的 `subPath` / `readOnly`；
- `StatefulSet.Spec.VolumeClaimTemplates` 仅含一个名为 `data` 的模板，`ApplyTraits` 返回的 additional objects 不含 standalone PVC；
- 默认 StorageClass 最终写入唯一 volumeClaimTemplate 的 PVC spec；不同名称或顶层非 persistent 同名 storage 不会错误共享 identity。

执行：

```bash
go test -race ./pkg/apiserver/domain/service/application ./pkg/apiserver/workflow/traits
go vet ./pkg/apiserver/domain/service/application ./pkg/apiserver/workflow/traits
git diff --check
```

## 发布与运维边界

- 后续行为变更仍需按仓库发布规则同步版本与文档。
- 仅新建或重新创建的 StatefulSet 会使用修复后的 PVC 语义；既有 PVC 不自动改 StorageClass，也不自动删除历史多余 PVC。
- 上线前核对目标 Eruun 版本已包含本修复，并在隔离测试 namespace 验证只出现一个 MySQL 数据 PVC，且其 StorageClass 与请求一致。
