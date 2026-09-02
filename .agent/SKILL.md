---
name: eruun-code
description: "Use only for Eruun repo code work: implementation, review, tests, builds, dependencies, Go toolchain/config behavior, behavior docs, devlogs, or PR text. Do not use for unrelated ~/.codex skill maintenance."
---

# Eruun Code

## Core Contract

- Do not multiply entities beyond necessity.
- Prefer existing package boundaries, helpers, config paths, and docs structure.
- Prefer fail-fast behavior over fallback/degrade paths unless the requirement explicitly needs compatibility behavior.
- Start broad repo orientation at `docs/README.md`; follow its routing before deep exploration.

## Execution Gate

For changes to repo-tracked code, docs, config, manifests, examples, or tests:

1. First produce Phase A analysis with impact surface, behavior contract, risks, test plan, and docs/devlog assessment.
2. Implement only after explicit user confirmation such as `确认实现`, `开始实现`, `按方案执行`, `implement now`, or `go ahead`.
3. Low-risk read-only tasks, reviews, command output explanations, and PR text drafting do not need the implementation gate.

Use [docs/context-packet.md](docs/context-packet.md) for the Phase A template and request-shaping examples.

## Implementation Rules

- Read the touched layer before editing; avoid speculative rewrites.
- Keep code small, cohesive, and convention-matched: early returns, narrow interfaces, package-local helpers when possible.
- Use lowercase package paths and Go 1.25-compatible style.
- Use `k8s.io/klog/v2` structured logging for new logs.
- Wrap errors with operation context, for example `fmt.Errorf("create pvc: %w", err)`.
- Pass `context.Context` through goroutines and prefer `errgroup` or `sync.WaitGroup`.
- Avoid silent retries, default substitution, swallowed errors, empty-value substitution, and best-effort continuation unless explicitly required and made observable.

## Repo Routing

- HTTP/API: `pkg/apiserver/interfaces/api`, DTOs in `interfaces/api/dto/v1`, assemblers in `interfaces/api/assembler/v1`.
- Business rules and lifecycle: `pkg/apiserver/domain/service`, models in `domain/model`, repository contracts in `domain/repository`.
- Workflow scheduling/execution: `pkg/apiserver/event/workflow`; resource reconciliation in `event/workflow/job`.
- OAM traits and naming: `pkg/apiserver/workflow/traits` and `workflow/naming`.
- External adapters: `pkg/apiserver/infrastructure`.
- Shared helpers: `pkg/apiserver/utils`, only for reusable technical utilities.

## Testing

- Prefer focused package tests for narrow changes; broaden to `go test ./... -race -cover` for shared contracts or cross-layer behavior.
- Use table-driven tests and cover error paths, edge cases, and regression examples.
- If a full test cannot run, run the smallest meaningful check and state what remains unverified.
- For review-driven fixes, include the path that failed, the corrected path, and at least one adjacent regression case when feasible.

## Docs And Devlogs

- Update docs when behavior, API, config, workflow contracts, manifests, or operational usage changes.
- Link new focused docs from repo `docs/README.md`.
- Add a `devlogs/` decision record when there is a meaningful technical choice, cross-layer contract, migration/compatibility risk, or non-obvious tradeoff.
- Exempt pure tests, formatting, comments, typo fixes, and small local bug fixes when no meaningful design alternative exists; state the exemption briefly.

## PR Hygiene

- Update PR descriptions only when new commits change scope, user-visible behavior/API, configuration, or risk.

## References

- [docs/context-index.md](docs/context-index.md) - compact repo concept and context map.
- [docs/context-packet.md](docs/context-packet.md) - request packet and Phase A template.
