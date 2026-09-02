# CloudJob 热插拔 Provider 设计方案

> 状态：Draft / Proposal。本文描述 provider 热插拔演进方案，不代表当前支持运行时加载外部 provider。

## 1. 背景与目标

当前 `cloudjob` provider 采用进程内静态注册，新增或替换 provider 需要重启 apiserver。  
本方案目标是在不重启 apiserver 的前提下，实现 provider 的动态加载、替换和下线。

目标如下：

- 支持 provider 插件热插拔
- 保持 action 严格白名单
- 保证运行中任务执行一致性
- 保持现有内置 provider 兼容

## 2. 设计结论

- 热插拔形态：外置进程插件
- 插件发现方式：目录清单监听
- 进程托管：由 apiserver 托管插件进程
- 切换语义：运行中任务绑定旧版本，新任务走新版本

## 3. 总体架构

核心组件：

- Provider Registry：统一 provider 查询入口（内置 + 动态）
- Plugin Runtime Manager：监听清单目录、启停插件进程、维护版本路由
- Plugin Client：与插件通过 gRPC + UDS 通信
- Job Cloud Controller：执行时冻结 provider revision 并写入 `job.Info`

数据流：

1. Runtime 监听清单目录，发现新插件后拉起进程并握手。
2. 插件返回 `provider/revision/supportedActions` 后注册到动态路由表。
3. `cloudjob` 执行时先选定 provider revision，再执行 action。
4. 插件更新后，新任务使用新 revision，旧任务继续绑定旧 revision。
5. 旧 revision 在无任务引用后回收。

## 4. 插件清单与协议

### 4.1 清单字段（manifest）

建议字段：

- `provider`
- `revision`
- `command`
- `args`
- `env`
- `workDir`
- `startupTimeout`
- `stopGracePeriod`

### 4.2 插件 RPC（gRPC）

必需接口：

- `GetMetadata`：返回 provider/revision/supportedActions
- `ValidateAction`：参数校验
- `RunAction`：一步执行，返回 `done/state/result/requeueAfter`

可选接口：

- `Health`：健康检查

## 5. 接口与数据结构变更

在 `cloudjob/contracts` 中：

- `CloudJobRequest` 新增 `ProviderRevision string`

在 `job` 执行记录中：

- `CloudJobRecord` 新增 `providerRevision`，持久化到 `job.Info`

错误模型：

- 增加可重试插件错误（如连接断开、短时不可达），用于重试与观测。

## 6. 执行一致性与故障策略

一致性策略：

- 任务首次运行时绑定 provider revision
- 后续重试与续跑固定该 revision，不跨版本漂移

故障策略：

- 插件短暂不可用：返回可重试错误
- action 不在白名单：直接 `ErrCloudActionNotFound`
- revision 已清理且仍有绑定任务：明确失败并记录诊断信息

## 7. 配置项

建议新增配置（flag + `ERUUN_` 环境变量）：

- `cloud-provider-plugin-enabled`
- `cloud-provider-plugin-dir`
- `cloud-provider-runtime-dir`
- `cloud-provider-plugin-call-timeout`
- `cloud-provider-plugin-drain-timeout`

## 8. 兼容性与迁移

- 保留内置 provider（如 aliyun）作为兼容路径
- 动态插件能力通过开关启用
- `job.Info` 新增字段为可选，兼容历史记录反序列化

## 9. 测试计划

单元测试：

- 清单增删改触发插件加载、替换、下线
- 握手失败、进程崩溃、重连与状态恢复
- 版本绑定与路由一致性
- 白名单检查与错误语义

集成测试：

- 热更新插件后二次任务切换到新 revision
- 运行中任务不受更新影响
- 旧 revision 引用归零后正确回收

回归测试：

- `go test ./pkg/apiserver/event/workflow/cloudjob/...`
- `go test ./pkg/apiserver/event/workflow/job -run 'CloudJob'`
- `go test ./...`

## 10. 分阶段落地

Phase 1：

- 定义清单格式与 gRPC 协议
- 完成 Runtime Manager 基础能力

Phase 2：

- 接入 `cloudjob` 执行链路与 revision 冻结
- 补齐错误模型和观测指标

Phase 3：

- 增加回收与灰度能力
- 完成文档与部署模板
