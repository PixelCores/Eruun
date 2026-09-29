# CloudJob Custom Provider 扩展指南

> 状态：Implemented Reference。当前 custom provider 模板位于 `pkg/apiserver/event/workflow/cloudjob/custom`，但不会自动注册为运行时 Provider。

## 目标

本文档说明如何基于现有 `cloudjob/contracts` 接口扩展自定义 provider，并给出仓库内可直接复用的模板：

- 模板目录：`pkg/apiserver/event/workflow/cloudjob/custom`
- 示例 action：`custom.echo`
- 默认策略：严格白名单（未注册 action 直接失败）

## 模板结构

建议按如下结构组织自定义 provider：

- `constants.go`：provider/action/param/state 常量
- `provider.go`：实现 `cloudjob.CloudProvider`，并在 provider 内维护 action 白名单
- `runtime.go`：provider runtime 适配占位实现
- `action_*.go`：每个 action 一个文件（避免聚合大文件）
- `*_test.go`：provider/action 单测

当前模板实现文件：

- `pkg/apiserver/event/workflow/cloudjob/custom/constants.go`
- `pkg/apiserver/event/workflow/cloudjob/custom/provider.go`
- `pkg/apiserver/event/workflow/cloudjob/custom/runtime.go`
- `pkg/apiserver/event/workflow/cloudjob/custom/action_echo.go`
- `pkg/apiserver/event/workflow/cloudjob/custom/provider_test.go`

## 接入步骤

1. 复制 `custom` 包作为你的 provider 起点，重命名 `ProviderName` 和 action 常量。
2. 在 `provider.go` 的 action map 中显式注册你允许的 action，并通过 `ResolveAction` / `SupportedActions` 暴露统一行为。
3. 在 `runtime.go` 中接入真实依赖，并实现 `CloudRuntime.Call`。
4. 按 action 维度拆分文件，在 `Run` 中维护 `state + requeueAfter` 状态机。
5. 在初始化入口注册 provider：`cloudjob.RegisterCloudProvider(yourprovider.NewProvider())`。

说明：模板包不会自动注册为内置 provider，避免影响现有运行时行为。

## 示例注册代码

```go
import (
    wfcloudjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob"
    "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/custom"
)

func init() {
    wfcloudjob.RegisterCloudProvider(custom.NewProvider())
}
```

## 示例组件配置

```json
{
  "name": "custom-echo-demo",
  "type": "cloudjob",
  "properties": {
    "cloud": {
      "provider": "custom",
      "action": "custom.echo",
      "params": {
        "message": "hello from cloudjob"
      }
    }
  }
}
```

## 扩展约束与建议

- action 命名建议：`<provider>.<domain>.<verb>`，例如 `custom.storage.ensure_bucket`。
- 强约束白名单：只允许 provider action map 注册过的 action。
- `Validate` 中提前校验必填参数，避免把参数错误推迟到 SDK 侧。
- `Run` 中显式处理三类返回：
  - 完成：`Done=true`
  - 继续等待：`Done=false` 且 `RequeueAfter>0`
  - 错误：返回 error，交由上层任务失败处理
- `state` 仅保存可序列化、可重入的信息，避免保存瞬态对象。
- Provider 在 `NewRuntime` 中固定本次执行依赖，由返回的 runtime 持有。需要同次调用内重建 runtime 时，可使用 `CloudJobRequest.RuntimeProviderSnapshot`；该字段仅在内存中传递，不写入检查点。没有可信运行快照的持久化 state 不得按新配置静默恢复。
- 源码接入变更：已删除 `contracts.WithRuntimeProviderSnapshot` / `RuntimeProviderSnapshotFromContext`，自定义 action 如需瞬态快照应从传入的 request 读取；`WithDataStore` / `DataStoreFromContext` 和 Provider/Action 接口保持不变。内置 Aliyun 直接复用无状态 action，自定义模板仍保留工厂，可为需要实例状态的 action 单独构造。
- 新增 action 时同步补充单测：
  - 白名单解析
  - 参数校验失败路径
  - SDK 错误路径
  - 状态推进路径

## 推荐测试命令

```bash
go test ./pkg/apiserver/event/workflow/cloudjob/custom
go test ./pkg/apiserver/event/workflow/cloudjob/...
```
