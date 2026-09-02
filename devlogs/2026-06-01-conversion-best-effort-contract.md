# Conversion Best-Effort Contract

## 背景与需求

Q-007 指出 conversion/import 中存在大量 warning + skipped 分支，问题核心不是代码是否应该继续拆分，而是转换遇到无法表达、缺字段、无法匹配或有损映射的 Kubernetes 资源时，应该 fail-fast 还是继续产出可用组件。

本次产品决策明确为：conversion 采用 best-effort 契约。可转换的组件继续返回，不可完整映射的资源或字段跳过并通过 warning 暴露。

## 影响范围

- API: `/applications/convert` 保持现有请求/响应结构，`warnings` 成为成功响应中的契约字段。
- Domain: conversion 行为保持 best-effort，不新增 strict 模式。
- DB: 无变化。
- Cache: 无变化。
- K8s: 无资源生成或 apply 行为变化。
- Workflow: 无变化。

## 技术选型与取舍

采用“显式化现有 best-effort 行为”的方案，而不是新增 strict/loose 开关。原因是转换能力面向已有 Kubernetes YAML 的反向建模，输入资源可能混杂、孤立或超出 Eruun 当前模型；只要有可转换组件，继续返回并展示 warning 更符合当前使用方式。


## 实现摘要

- 保持 conversion 代码行为不变，新增回归测试固定 warning 不会自动导致转换失败。
- 文档明确 `valid=true` 只代表转换后的 Eruun Application 通过校验，不代表输入 YAML 被无损转换。
- namespace import 继续采用部分成功策略，通过 `warnings` 和 `resourceResults[].status/error` 表达转换或打标阶段的跳过和失败。
- 审计文档将 Q-007 更新为已解决，后续代码简化需保留该契约。

## 测试与验收

- `go test ./pkg/apiserver/domain/service/conversion`
- `go test ./pkg/apiserver/domain/service/namespaceimport`

## 风险与后续

- 调用方若只看 `valid=true` 而忽略 `warnings`，仍可能误认为转换无损；文档已把 warnings 标为必须检查的结果字段。
- 当前 warning 仍是字符串数组；若后续需要机器可判定的 skipped 分类，可新增结构化结果字段作为兼容扩展。
