# Context Packet (任务上下文包)

提需求时一次性提供"最小且充分"的上下文，减少追问，提升单轮交付成功率。

---

## 快速模板

```markdown
### Goal (目标)
一句话描述期望效果

### Scope (范围)
- 文件: `path/to/file.go`
- 符号: `func Foo()`, `type Bar`

### Current → Expected (现状与期望)
- 现状:
- 期望:
- 错误日志（如有）:

### Risk (风险)
Low / Medium / High

### Constraints (约束，可选)
- 兼容性:
- 性能要求:

### Deliverables (期望输出)
- [ ] 代码
- [ ] 测试
- [ ] 文档
- [ ] Devlog
- [ ] PR 文案
```

---

## Phase A 模板（先分析，不改代码）

> 用于 `eruun-code` 两阶段流程的第一阶段。
> 目标：先锁定影响面与契约，再进入实现。

```markdown
### Change Goal
一句话描述本次变更目标

### Impact Surface (API/Domain/DB/Cache/K8s)
- API:
- Domain:
- DB:
- Cache:
- K8s:

### Contract Draft (关键字段/行为)
- 字段/行为 1: 表示/编码/脱敏/优先级
- 字段/行为 2: 表示/编码/脱敏/优先级

### Risks
- 风险点:
- 回归点:

### Test & Acceptance
- 单测:
- 集成/手工验证:
- 验收口径:

### Devlog Assessment
- 是否需要 `devlogs/` 记录:
- 理由:
- 计划文件名:
```

### 进入 Phase B 的确认语

需要用户显式确认后才进入实现阶段，推荐使用：

- `确认实现`
- `开始实现`
- `按方案执行`
- `implement now`
- `go ahead`

---

## 填写示例

```markdown
### Goal
修复 CleanupResources 后组件状态显示 Unknown 而非 Not Deploy

### Scope
- 文件: `pkg/apiserver/domain/service/application.go`
- 符号: `func (a *ApplicationService) CleanupResources()`

### Current → Expected
- 现状: 清理资源后状态变为 Unknown
- 期望: 清理资源后状态变为 Not Deploy
- 错误日志: 无

### Risk
Low（仅影响状态显示，无持久化变更）

### Constraints
- 必须兼容现有 API 响应格式

### Deliverables
- [x] 代码
- [x] 测试
- [ ] 文档
- [ ] Devlog（纯状态展示修复，无重要技术选型时可豁免并说明）
```

---

## 最小信息集

当不确定提供什么时，至少包含：

1. **Goal** - 一句话目标
2. **Scope** - 文件路径 + 关键符号
3. **Current → Expected** - 复现步骤或错误输出
4. **Risk** - 你的判断

---

## 提供代码片段的原则

| ✅ 做 | ❌ 避免 |
|------|--------|
| 仅贴关键片段 (30-120行) | 整文件粘贴 |
| 标注路径和符号名 | 无上下文的代码块 |
| 包含调用方/被调用方 | 仅贴单个函数 |
