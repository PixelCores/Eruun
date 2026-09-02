# 应用状态一致性

## 背景与需求

应用状态存在两类可复现的代码级错误模型：显式 stop 已把 managed workload 写为 `Stopped`，但共享 proxy 的 `Running` 仍把应用聚合为 `running`；managed workload 已全部 Ready，但共享 proxy 在当前应用的组件记录仍为 `Pending`，导致应用持续显示 `pending`。

进一步审计发现，状态接口读取 24 小时组件缓存、同组件 Informer 事件可由多个 worker 乱序完成、Informer 写入只按组件身份更新而没有比较读取到的旧运行态。这些问题彼此独立，任一处都可能让响应和最近一次有效生命周期操作不一致。

本变更只依据代码和自动化测试修复，不读取集群或数据库，也不据此对某次线上请求作确定性归因。

## 影响范围

- API：三个既有 status 路径、DTO、状态枚举和响应结构不变；读取改为绕过普通组件缓存。
- Domain：应用普通 availability 优先聚合 managed webservice；全局高优先级仍检查全部组件。
- DB：不新增列、不收紧字段约束；启动时在 AutoMigrate 后幂等归一化历史运行态 NULL，并增加基于旧运行态快照的 Informer 专用 compare-and-set。
- Cache：普通组件缓存只作加速，并在状态同步完成后的所有出口统一失效。
- K8s：不改变资源、副本数、Pod 总数或 Ready 判定语义。
- Workflow：显式 stop/start/restart/version 等生命周期写入继续是权威写入。

## 技术选型与取舍

### 状态查询使用 fresh repository read

状态接口需要表达最新持久化运行态，不能依赖普通 Components API 的 24 小时缓存。API 层为此声明只包含 runtime component 查询能力的私有窄 reader，并由 IoC 使用同一个 application service 实例独立注入；导出的 `ApplicationsService` 方法集保持兼容。三个状态路径通过该 reader 复用组件准备及 ready 副本纠正逻辑但不读写组件缓存，也不回退到缓存查询；普通 `/components` 接口继续保留缓存。

### 按生命周期归属聚合 availability

`share=default` / `share=ignore` 的 workload 不由当前应用独立控制，因此不参与 managed availability。未配置 share 和 `share=force` 仍参与。全局失败和操作中状态继续检查全部组件；只有共享 webservice 时回退现有共享组件状态，避免把“共享资源存在”当作 Ready。

本次不实现跨 APP 的共享健康状态扇出，也不主动把复用的共享组件记录改写为 `Running`。

共享生命周期归属使用无日志副作用的纯分类读取。未知策略继续按 `default` 归一化，但状态轮询不会为每个组件重复输出 fallback 告警；显式生命周期和 workflow 调和仍保留各自的操作日志。

### keyed latest-only lane 与真实 CAS

同一 `(appID, componentID)` 只允许一个 drain，排队期间的中间快照合并为最新值；不同组件继续由现有两个 worker 并行。executor overflow 只触发同一 lane 的重新提交，不再绕过队列直接并发执行。Informer Manager 的 handler 捕获当前 runtime generation；reset/Stop 等待已经开始的旧 handler 和状态回调退出，并通过内部 epoch 丢弃尚未执行的旧 generation 更新，不扩展公共事件结构。

队列排序解决旧 observation 晚完成的问题；CAS 解决 observation 读取后显式生命周期写入抢先提交的问题。CAS 同时比较组件身份与旧 `status`、`ready_replicas`、`last_abnormal`；冲突代表旧事件，直接丢弃且不重试。`update_time` 是组件整行的通用更新时间，image、traits、properties 等非运行态写入也会推进它，因此不能作为运行态 generation 或 CAS 条件。队列与 CAS 两层保护不可相互替代。

历史表可能在运行态字段首次加入时留下 NULL，而 Go 模型会把这些值读成零值，导致按零值构造的等值 CAS 无法命中 SQL NULL。初始化顺序因此保持三字段 nullable：先由 AutoMigrate 补齐可能缺失的列，再无条件执行三项幂等更新，把 `status`、`ready_replicas`、`last_abnormal` 的 NULL 分别回填为 `''`、`0`、`''`。回填不使用只返回 bool 的 table/column schema probe 作为跳过依据，不修改 `update_time`；任一更新失败都会带列名上下文中止 datastore 初始化，且不继续后续回填。

## 实现摘要

- 三个状态 handler 通过 API 私有窄 reader 调用 fresh runtime component 查询，导出的 `ApplicationsService` 方法集不变。
- availability 聚合排除 `share=default` / `share=ignore`，保留 `share=force`。
- Informer 状态队列增加 keyed single-drain、latest-only 和 reset epoch。
- Manager runtime generation 为 reset/Stop 和旧 handler/状态回调建立进程内栅栏；leader-scoped 停止路径同步等待 Informer Stop 完成后才允许下一轮 Start。
- Informer 普通回写及 `Cleaning -> Not Deploy` 使用运行态三元组旧值 CAS；通用 `update_time` 不参与比较。
- datastore 在 AutoMigrate 后无条件、幂等归一化历史运行态 NULL；任一回填错误都会中止初始化，runtime repository 拒绝写入新的 nil 值。
- 状态同步完成、冲突或终态跳过后统一失效普通组件缓存。

## 测试与验收

- 聚合覆盖 shared default/ignore、share force、仅共享组件和共享组件全局失败。
- API 覆盖旧形态实现仍满足导出的 `ApplicationsService`、生产 service 可注入私有窄 reader，以及缓存旧 `Running`/`Pending`、repository 新 `Stopped`/`Running` 时三个状态接口均返回 repository 状态。
- 队列覆盖同 key 串行、latest-only、overflow 不越序、不同 key 并行和 reset 丢弃旧 generation。
- generation 覆盖旧 handler 拒绝、reset/Stop 等待已开始回调，以及栅栏返回后旧回调不能启动。
- CAS 覆盖并发权威 `Stopped` 写入、并发非运行态配置写、`Cleaning`、记录缺失、冲突和无变化更新；相同 Pod snapshot 不会再次提交状态回调。
- share 分类覆盖 nil、空、default、ignore、force、未知和异常 traits，并验证状态轮询分类不输出重复告警。
- MySQL DryRun 覆盖三项 NULL 回填 SQL、三项更新无条件执行、第二项更新失败时错误带列名并阻止第三项执行，以及新组件显式写入运行态零值；未连接真实数据库。
- 运行聚焦包 race 测试、全仓 `go test ./... -race -cover`、`go vet ./...`、格式化和 `git diff --check`。

## 风险与后续

- 共享-only 应用仍依赖当前应用组件记录中已经持久化的共享状态；本变更不建立共享 workload 到所有引用 APP 的状态扇出。
- 普通 Components API 继续是缓存加速接口，不承诺与三个 status 接口相同的强实时性；本变更把状态同步触发的失效移到同步结束，但不为普通组件缓存增加版本化写保护。
- runtime generation 是进程内栅栏，不是跨实例分布式 fencing token。多实例 leader 交接时，跨进程旧写主要由持久化运行态快照 CAS 拦截；若两个写入的运行态三元组完全相同，现有模型无法区分其语义代次。若要严格跨进程全序，需要独立、原子递增的 runtime revision。
- 历史 NULL 回填只更新为当前零值语义且不推进 `update_time`；缺表、缺列、权限、连接或更新错误不再静默跳过，而会阻止 datastore 启动。本次使用 GORM MySQL DryRun 验证 SQL 和失败路径，没有连接真实数据库执行迁移集成测试。
- 不调整 Pod 总数与期望副本数的既有含义，不扩展 Updating/Restarting 状态机。
- 若未来需要跨 APP 展示共享资源实时健康，应建立独立、可关联的共享运行态事实源，而不是把资源存在直接等同于 Ready。
