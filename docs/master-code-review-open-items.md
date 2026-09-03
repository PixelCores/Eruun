# 代码评审统一清单（2026-05-05）

> 历史认证条目已被统一账号和空间契约取代；当前行为见 [账号与空间](account-auth-workspaces.md)。


> 状态：Historical / Audit。本文用于跟踪历史 review 结论，不作为当前用户使用入口。

## 说明

- 本文是 `docs/` 下 review、审计、历史项目分析与依赖安全处理记录的统一入口。
- 本文保留仍需跟踪的事项，并记录已经闭环或过期的历史结论；被合并的旧文档不再单独维护。
- 复核基线：当前工作区代码（2026-05-05）。
- 复核方式：逐项回到代码核对，并执行窄范围回归测试。
- 本文主要整合历史文档；本轮同时补齐工作流调度查询索引。

## 已合并文档

- 本文原有内容：`docs/master-code-review-open-items.md`
- `docs/review-phase3-q005-q007-s002-s004-s006.md`
- `docs/workflow-schedule-optimization.md`
- `docs/project-analysis-report.md`
- `docs/dependabot-alerts-resolution.md`
- 以下已由旧版 `master-code-review-open-items.md` 吸收的历史文档继续视为已合并：
  - `docs/master-code-review-2026-02-18.md`
  - `docs/master-code-review-2026-03-11.md`
  - `docs/master-code-review-2026-03-13.md`
  - `docs/master-code-review-2026-04-09.md`
  - `docs/master-kafka-queue-audit-2026-04-21.md`
  - `docs/master-redundancy-refactor-plan-2026-03-20.md`
  - `docs/master-unused-code-review-2026-03-18.md`
  - `docs/master-unused-code-review-2026-03-19.md`
  - `docs/master-code-review.md`

## 状态定义

| 状态 | 含义 |
| --- | --- |
| Open | 问题仍存在，建议进入后续修复计划。 |
| Partial | 已有治理，但仍有残留风险或范围未完全覆盖。 |
| Closed | 当前代码已闭环，保留记录避免重复提出。 |
| Obsolete | 旧文档描述的对象或结论已过期，不再作为行动项。 |
| Recommendation | 优化建议，不是当前可证实缺陷。 |

## 当前仍需跟踪

| ID | 状态 | 来源 | 当前结论 | 代码证据 | 建议方向 |
| --- | --- | --- | --- | --- | --- |
| S-003 | Partial | `master-code-review-open-items.md` / 历史 `2026-02-18 优化-5` | API 已接入 `RequestBodyLimit`、`Auth` 与可配置 `RateLimit`，但限流默认关闭，未见全局请求并发上限。旧结论“只有 RequestBodyLimit”已过期，但“统一请求保护能力仍不完整”仍成立。 | `pkg/apiserver/server.go:373-381`、`pkg/apiserver/config/config.go:153-154`、`pkg/apiserver/config/config.go:223-228` | 保持现有中间件入口，补充全局并发上限或明确不做全局并发限制的产品约束；避免再新增散点保护逻辑。 |
| Q-007 | Partial | `review-phase3-q005-q007-s002-s004-s006.md` | `namespace_import` 扫描职责已拆出，但 `namespace_import.go` 仍约 3100 行，长文件/多职责问题仍部分存在。 | `pkg/apiserver/domain/service/namespace_import.go`、`pkg/apiserver/domain/service/namespace_import_scan.go` | 继续按现有边界拆分导入计划构建、资源分组、组件转换、响应标记等职责；每次拆分保持行为测试不变。 |

## 已闭环或已过期

### Phase-3 清单

| ID | 状态 | 当前结论 | 代码证据 |
| --- | --- | --- | --- |
| Q-005 | Closed | Job result enqueue 失败不会进入本地执行器；outbox 保持在队列状态机中并由固定结果队列路径重试。 | `pkg/apiserver/event/workflow/job/job_result_outbox.go` |
| S-006 | Closed | Informer 状态同步已接入 `BoundedExecutor`，并有 overflow replay 路径。 | `pkg/apiserver/infrastructure/informer/waiter.go:33-34`、`pkg/apiserver/infrastructure/informer/waiter.go:100-105`、`pkg/apiserver/infrastructure/informer/waiter.go:636-650` |
| S-002 | Closed | `scanNamespaceResources` 已委托到表驱动扫描流程。 | `pkg/apiserver/domain/service/namespace_import.go:1082-1083`、`pkg/apiserver/domain/service/namespace_import_scan.go:15-48` |
| S-004 | Obsolete | 当前代码库未发现旧文档描述的独立模板引擎扩展抽象文件；模板相关能力主要体现在应用模板克隆与模型定义中。 | `pkg/apiserver/domain/service/application_template_clone.go`、`pkg/apiserver/domain/model/template.go` |

### 工作流调度

| ID | 状态 | 当前结论 | 代码证据 |
| --- | --- | --- | --- |
| WS-001 | Closed | `DispatchWorkflowSchedules` 已要求 datastore 实现事务接口；不支持事务时直接返回错误，不再执行非事务入队 fallback。命中调度窗口时，`next_run` CAS claim、应用工作流空闲检查、队列任务创建与 `last_run` 更新在同一事务内提交。 | `pkg/apiserver/domain/service/workflow.go:657-660`、`pkg/apiserver/domain/service/workflow.go:736-763` |
| WS-002 | Closed | `WorkflowSchedule` 已通过 GORM tag 声明 `idx_workflow_schedule_enabled_next_run` 复合索引，字段顺序为 `enabled,next_run`，与 `FindEnabledWorkflowSchedules` 的过滤和排序路径匹配。 | `pkg/apiserver/domain/model/workflow_schedule.go:13-14`、`pkg/apiserver/domain/model/workflow_schedule_test.go`、`pkg/apiserver/domain/repository/workflow_schedule.go:70-73` |
| WS-003 | Closed | `WorkflowQueue` 已新增 `IdempotencyKey` 唯一索引；调度入队路径使用 `workflowScheduleIdempotencyKey(schedule.ID, claimedNextRun)` 表达同一调度 tick 的唯一性，重复入队冲突会按 idempotency key 查询并复用已有任务。 | `pkg/apiserver/domain/model/workflow_queue.go:13`、`pkg/apiserver/domain/service/workflow.go:821-822`、`pkg/apiserver/domain/service/workflow.go:1529-1539`、`pkg/apiserver/domain/service/workflow_test.go:2333-2417` |

### 旧项目分析报告

| 旧问题 | 状态 | 当前结论 | 代码证据 |
| --- | --- | --- | --- |
| `ListApplicationWorkflow` panic | Closed | 对应能力已在 application service 中实现为 `ListApplicationWorkflows`，未发现生产路径中的 `panic("implement me")`。 | `pkg/apiserver/domain/service/application_query.go:684-706` |
| `startWorkers/stopWorkers` 竞态 | Closed | worker 启停已使用 `workersMu` 保护。 | `pkg/apiserver/server.go:603-624` |
| 默认 MySQL 密码 `root:123456` | Closed | 默认 `Datastore.URL` 为空，MySQL 类型下强制校验非空。 | `pkg/apiserver/config/config.go:166-169`、`pkg/apiserver/config/config.go:229-231` |
| 工作流队列类型断言 panic | Closed | 相关 repository 已使用安全类型断言并记录 warning。 | `pkg/apiserver/domain/repository/workflow.go:301-304`、`pkg/apiserver/domain/repository/workflow.go:356-359`、`pkg/apiserver/domain/repository/workflow.go:374-377` |
| 缺少认证授权中间件 | Closed | API 路由已接入 `middleware.Auth`，并从 `apiAuth` 系统设置加载 JWT/RBAC 策略。 | `pkg/apiserver/server.go:381-387`、`pkg/apiserver/interfaces/api/middleware/auth.go` |
| CORS `*` 过宽 | Closed | 当前 `AllowCredentials=false`，旧报告也已判定该组合不是凭证型跨站风险。 | `pkg/apiserver/server.go:365-371` |
| Redis cache 全局 client | Closed | `utils/cache` 已改为实例持有 redis client，并通过 `GetRedisClient` 暴露给依赖注入路径。 | `pkg/apiserver/utils/cache/redis_cache.go:10-17`、`pkg/apiserver/utils/cache/redis_cache.go:130-132` |
| 全局依赖使用过多 | Closed | `globalWaiter`、`resultQueue`、`delayQueue` 已从运行期全局句柄收敛为显式注入或显式队列入参；Job 控制器通过 `jobRuntime` 接收 waiter/延迟队列，result dispatcher/outbox 继续使用自身持有的队列字段。 | `pkg/apiserver/event/workflow/job/job.go`、`pkg/apiserver/event/workflow/job/job_controller_base.go`、`pkg/apiserver/event/workflow/job/delay_queue.go`、`pkg/apiserver/event/workflow/job/job_result.go` |
| `context.Background()` 使用 | Recommendation | 仍有若干脱离请求生命周期的后台/持久化 context，部分是后台工作需要，部分仍可继续收敛；当前不作为单点缺陷处理。 | `pkg/apiserver/domain/service/workflow.go:1132-1150`、`pkg/apiserver/event/workflow/job/job_result_outbox.go:640-648` |
| 错误处理/日志格式不统一 | Recommendation | 项目仍混用 `bcode`、`fmt.Errorf`、直接返回与多种 klog 风格；这是持续治理项，不是本次 review 的独立缺陷。 | 多处 |

### 依赖安全记录

| 项 | 状态 | 当前结论 | 代码证据 |
| --- | --- | --- | --- |
| Go toolchain | Closed | `go.mod` 已声明 `toolchain go1.25.8`。本地当前执行 `go version` 为 `go1.26.2 darwin/arm64`。 | `go.mod:3-5` |
| `golang.org/x/crypto` | Closed | 已升级到 `v0.45.0`。 | `go.mod:115` |

### Kafka 与历史 master review

- Kafka readiness 已补齐真实 `dispatch/delay/result` topic 的 produce/read smoke check，Kafka 探测职责已收敛到统一入口。
- `master-code-review-2026-03-11.md`、`master-code-review-2026-03-13.md`、`master-code-review-2026-04-09.md` 等历史清单中的未列出问题，当前视为已闭环或已过期，不再单独跟踪。

## 验证记录

已执行：

```bash
go test ./pkg/apiserver/domain/model
go test ./pkg/apiserver/domain/service ./pkg/apiserver/domain/repository ./pkg/apiserver/infrastructure/informer ./pkg/apiserver/event/workflow/job ./pkg/apiserver/interfaces/api/middleware
```

结果：全部通过（部分 cached）。

真实 MySQL 环境可执行以下 SQL 验证执行计划：

```sql
SHOW INDEX FROM eruun_workflow_schedule
WHERE Key_name = 'idx_workflow_schedule_enabled_next_run';

EXPLAIN
SELECT id, app_id, workflow_id, cron, enabled, next_run, last_run, create_time, update_time
FROM eruun_workflow_schedule
WHERE enabled = true
ORDER BY next_run ASC;
```

本次未执行全量：

```bash
go test ./... -race -cover
go vet ./...
```

## 后续建议

1. 继续推进 `Q-007`：按职责逐步拆分 `namespace_import.go`，每次只移动稳定 helper，并跑 namespace import 相关测试。
2. 若要启用 API 限流，补齐部署文档中的 `--api-rate-limit-qps` 与 `--api-rate-limit-burst` 示例；若需要更强保护，再考虑全局并发上限中间件。
