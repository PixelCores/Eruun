# Code Quality Audit Action Map

> 状态：Current。本文把全仓代码质量审计项映射到可独立推进的小 PR；当前事实优先使用 `go-idiomatic-code-quality-audit-2026-08-09.md`，五月报告只作历史基线。

## 事实源

- `go-idiomatic-code-quality-audit-2026-08-09.md`：当前 `master` 快照、全 package 覆盖、旧项复核与 Q-016 至 Q-021。
- `code-quality-audit-2026-05-20.md`：Q-001 至 Q-015 的原始发现与当时状态。
- `project-style-guide.md`：项目命名、分层、错误、context、并发和文档维护基线。

## 使用规则

- 每个 PR 保持单一边界；结构重构不得混入 strict/loose、fallback、错误码、nil/empty、事务、callback 或状态机语义变化。
- 先使用现有 package、类型和 helper；只有出现第二个真实消费者时才增加共享抽象。
- 优先做同 package 文件/helper 拆分，再做跨层接口或持久化迁移。
- 行为类变更必须先写失败路径与契约测试；并发/全局状态变更必须运行 `-race`。
- 已关闭或 obsolete 的旧项不重新实施；新证据应在新审计中重新打开并说明原因。

## 当前行动映射

| 审计项 | 当前状态 | 行动线 | 推荐 PR 形态 | 验证重点 |
| --- | --- | --- | --- | --- |
| Q-016 | Partial | 移除 domain -> transport 反向依赖 | programming-language 链路已改为 domain command/model，并把 DTO mapping 留在 interfaces；后续按链路迁移，不先建通用 usecase framework | route -> DTO -> assembler -> service -> persistence -> tests -> docs |
| Q-017 | Partial | datastore typed intent 试点 | application batch lookup 已由 `ApplicationRepository.FindByIDs` 承接；继续逐个收敛高频 string/map query，保留主 DataStore 兼容 | 表名、primary key、CAS、事务、rows affected、error identity |
| Q-018 | Partial | 窄 consumer interface + 有效构造 | namespace import/event workflow 已用窄接口；programming-language repository/service 已提供 fail-fast 显式构造并由 server assembly 使用 | fake 规模、缺失依赖 fail-fast、server assembly 与 API 回归 |
| Q-019 | Partial | 全局 registry 实例化 | model 已移除 `init`/全局 registry，改由 `BuiltinModels` 向 server assembly 返回有序快照；API/trait/provider 后续分别迁移 | 顺序、重复注册、并行测试、server 多实例、`-race` |
| Q-020 | Partial | context owner 收敛 | server lifecycle 已拒绝 nil context；继续逐 package 移除内部 fallback，并保留明确 timeout 的 persistence/shutdown/callback context | cancel、deadline、unlock、callback、terminal write、worker shutdown |

## 推荐推进顺序

1. **跨层单链路试点**：组合 Q-016 与 Q-018 的一个低风险消费者，但不要同时迁移全仓 service。
2. **持久化边界**：Q-017 单 repository 试点，保持 datastore wire/storage 行为。
3. **初始化与并发高风险**：Q-019、Q-020 分开推进并运行全量 race test。

## 已关闭或过期的旧项

| 审计项 | 状态 | 说明 |
| --- | --- | --- |
| Q-002 | Closed | `GenerateJobTasks` 已收敛为薄编排入口，复杂阶段位于同包 helper。 |
| Q-004 | Closed | validation 已按规则主题拆分。 |
| Q-006 | Closed | template clone/rewrite/override/traits 已分文件。 |
| Q-007 | Closed | conversion best-effort 契约已明确并有回归测试。 |
| Q-008 | Closed | cleanup discovery/delete/reporting 已拆分；新状态机只按具体证据审查。 |
| Q-012 | Closed | 当前 `gofmt -l` 无输出。 |
| Q-013 | Closed | Makefile 已有 `staticcheck`、`gocyclo`、`quality` target；工具安装是环境前置。 |
| Q-014 | Closed | 本行动映射持续维护审计到小 PR 的关系。 |
| Q-015 | Obsolete | 被引用的 `fallback-degradation-audit.md` 已不存在；后续 fallback 必须从当前代码和 Current 文档重新取证。 |
| Q-001 | Closed | namespace import 已拆为 prepare/response/verify/apply 阶段，ownership inference、reference assignment、grouping 与 presentation 已拆为命名 helper。 |
| Q-003 | Closed | version update 主入口已拆为 normalize/preflight、workflow selection、三路径 commit 与 response/finalize 阶段。 |
| Q-009 | Closed | server composition 已按 core、assembly、bootstrap、leader、worker 与 status sync 拆分为同包文件。 |
| Q-010 | Closed | DTO 已按 application/workflow/status/component/version 拆分；component assembler 已按 core/service/resource/credential/link 拆分。 |
| Q-011 | Closed | 原最大测试文件已按 create/exec、status、callback、cleanup、workflow、resource 等行为主题拆分，当前最大测试文件低于 1,800 行。 |
| Q-021 | Closed | `JSONStruct` 编码路径显式返回错误，吞错兼容方法和未使用反射 helper 已移除。 |

## 与架构草案的关系

`architecture-refactor-plan.md` 继续作为中长期 proposal。本行动映射不授权一次性新增 application/usecase framework、替换整个 IoC 或重写 datastore；先用一个调用链证明边界和收益，再决定是否推广。
