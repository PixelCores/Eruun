# Eruun 架构演进方案（应用层/依赖显式化/注册收敛/用例拆分）

> 状态：Draft / Proposal。本文是后续演进方案，不代表当前代码已经完成该架构拆分。

> 示例说明：本文结构图和代码片段均为概念伪代码，不可直接用于生成实现。

本文档提出一套可渐进落地的架构演进方案，解决以下问题：
- `domain/service` 直接依赖 `interfaces/api` 的 DTO 与 assembler，导致层间耦合偏重。
- 运行期 IoC 反射注入降低可读性与可验证性。
- 全局注册模式（traits/api/event）带来隐式状态与初始化顺序依赖。
- 领域服务职责过大，难以测试与隔离。

## 1. 目标与非目标

### 目标
- 让 `domain/service` 只处理领域模型与仓储接口，DTO/装配迁移到接口层或应用层。
- 核心路径引入构造函数注入，提升依赖可见性与编译期校验。
- 将全局注册改为显式注册列表/模块化工厂。
- 将大服务拆分为更细粒度用例（UseCase），提升测试性与隔离性。

### 非目标
- 不改变现有 API 行为与协议。
- 不一次性重构所有模块，采用分阶段迭代。

## 2. 现状问题摘要

1) 层间耦合
- `pkg/apiserver/domain/service/*` 中直接引用 `interfaces/api/dto` 与 `interfaces/api/assembler`。
- 领域层与传输层相互耦合，削弱可替换性与边界。

2) 运行期 IoC 注入
- `pkg/apiserver/utils/container` 基于反射注入，依赖链路隐式。
- 依赖错误在运行期发现，增加排查成本。

3) 全局注册
- `traits.RegisterAllProcessors()`、`api.RegisterAPI()`、`event` 全局 `workers`。
- 初始化顺序依赖隐蔽，测试时难以控制。

4) 服务职责过大
- `ApplicationsService` 同时处理 DTO、仓储、K8s 操作、工作流触发。
- 难以拆分测试场景，且对依赖变化敏感。

## 3. 目标架构（概念）

### 3.1 分层建议

```
interfaces/api  --->  app/usecase  --->  domain/service  --->  domain/repository
   DTO/Assembler       应用用例          领域逻辑             仓储接口
```

- **interfaces/api**：负责 HTTP 协议、DTO 校验、序列化、组装。
- **app/usecase**：对外的应用用例，协调领域服务与仓储，承接 DTO -> 领域模型的转换。
- **domain/service**：纯领域逻辑，仅依赖领域模型与仓储接口。

### 3.2 依赖注入建议

- 核心服务改为构造函数注入：
  - `NewApplicationUseCase(repo, kubeClient, workflowSvc, ...)`
  - `NewApplicationService(appRepo, workflowRepo, ...)`
- IoC 仅用于可选扩展或边缘组件，主路径坚持显式注入。

### 3.3 注册方式建议

- traits/api/event 改为显式列表或模块工厂：
  - `traits.NewProcessorSet()`
  - `api.NewRouterSet()`
  - `event.NewWorkers()`

## 4. 具体改造方案

### 4.1 引入应用层（app/usecase）

新增目录（建议）：
- `pkg/apiserver/app/usecase` 或 `pkg/apiserver/application`

示例职责：
- `ApplicationUseCase`
  - DTO -> 领域模型转换
  - 调用领域服务
  - 统一处理错误映射（如 bcode）

**变更要点**：
- `interfaces/api` 调用 usecase，而不是直接调用 domain/service。
- `domain/service` 只处理 `model` 与 `repository`。

### 4.2 DTO/Assembler 下沉到接口层

- 将 `interfaces/api/assembler` 仅保留在接口层。
- domain/service 直接返回领域模型，接口层负责 DTO 组装。

### 4.3 构造函数注入替代部分 IoC

- 在 `pkg/apiserver/server.go` 的组装流程中：
  - 用显式构造函数初始化核心对象
  - IoC 仅保留给可选模块或仍依赖反射注入的遗留对象

示例（概念）：
- `appUseCase := usecase.NewApplicationUseCase(appSvc, workflowSvc, kubeClient, store, ...)`
- `api.NewApplications(appUseCase)`

### 4.4 全局注册改为显式注册

- `traits.RegisterAllProcessors()` -> `traits.NewProcessorSet()` 返回处理器列表
- `api.RegisterAPI()` -> `api.NewRouterSet()` 返回路由注册集合
- `event.InitEvent()` -> `event.NewWorkers()` 返回 worker 列表

### 4.5 大服务拆分为用例

以 `ApplicationsService` 为例，拆分用例：
- `CreateApplicationUseCase`
- `UpdateApplicationWorkflowUseCase`
- `ListApplicationsUseCase`
- `CleanupResourcesUseCase`

每个用例只关注一个业务场景，输入输出为领域模型或接口 DTO。

## 5. 分阶段落地计划

### Phase 1：引入应用层与DTO下沉（低风险）
- 新增 `app/usecase` 层
- 迁移一条主链路（如 Create App）
- 调整接口层调用方式

### Phase 2：构造函数注入
- 优先替换核心依赖：API -> UseCase -> Service
- IoC 保留对非核心/遗留组件

### Phase 3：显式注册
- traits/api/event 改为显式列表或模块工厂
- 清理全局变量与隐式注册

### Phase 4：拆分大服务
- 将 `ApplicationsService` 拆成多个用例
- 补齐单测，按用例维度覆盖

## 6. 风险与缓解

- **风险**：改造涉及调用链变化，存在回归可能。
  - **缓解**：先迁移单一路径，保持 API 行为不变，补充用例测试。
- **风险**：构造函数注入调整较多。
  - **缓解**：仅替换核心路径，边缘模块保留 IoC。

## 7. 测试策略

- 用例级单测：针对新 `usecase` 编写表驱动测试
- 接口层回归：复用现有 API 测试
- 关键路径集成测试：Create App / Workflow 执行

## 8. 产出物清单

- 新增：`pkg/apiserver/app/usecase/*`
- 修改：`pkg/apiserver/interfaces/api/*` 调用 usecase
- 修改：`pkg/apiserver/domain/service/*` 去除 DTO 依赖
- 修改：`pkg/apiserver/server.go` 组装逻辑
- 修改：traits/api/event 注册方式
- 文档：本文件

---

如需进一步细化，可按模块拆分专项设计文档（如：应用层设计、注册机制设计、依赖注入替换清单）。
