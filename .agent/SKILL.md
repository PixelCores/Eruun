---
name: eruun-code
description: "Use only for Eruun repo code work: implementation, review, tests, builds, dependencies, Go toolchain/config behavior, behavior docs, devlogs, or PR text. Do not use for unrelated user-level skill maintenance."
---

# Eruun Code

## Clean-Change Contract

- Finish the requested work end to end with the smallest coherent change that is correct, readable, and verified.
- Do not multiply entities beyond necessity. Every new interface, type, package, helper, configuration switch, dependency, or fallback branch must solve a concrete present need.
- Reuse existing package boundaries, helpers, configuration paths, and documentation structure when they fit; do not force reuse when it obscures the behavior.
- Prefer direct control flow, concrete types, and explicit errors. Do not add speculative extensibility, compatibility aliases, silent fallback, or best-effort continuation.
- Preserve current API and data contracts unless the request explicitly changes them. Treat Draft and Proposal documents as future direction, not implemented behavior.
- Start broad repo orientation at `docs/README.md`; follow its routing before deep exploration.

## Working Method

1. Establish the current behavior from the narrowest relevant source, tests, and Current documentation. Trace callers or downstream consumers only as far as the contract requires.
2. Choose the simplest design that satisfies the request and identified regression risks. Prefer changing an existing path over creating a parallel path.
3. When the request and contract are clear, implement and verify the complete change in the same turn. Do not require a separate planning phase or confirmation ceremony.
4. Pause for user direction only when a missing choice would materially change public behavior, stored data, security, permissions, or the requested scope, or when the user explicitly asks for analysis only.
5. Before delivery, inspect the final diff once by default and remove accidental complexity, temporary code, dead paths, duplicated logic, unrelated formatting, and artifacts that exist only to satisfy process. Repeat inspection only for subsequent changes, failures, or a concrete unresolved risk, limited to the affected scope. Once required checks and this inspection pass, deliver.

Use [docs/context-packet.md](docs/context-packet.md) only when a task benefits from an explicit brief; do not require the user to fill it in.

## Implementation Rules

- Read the touched layer before editing; avoid speculative rewrites.
- Keep code small, cohesive, and convention-matched. Prefer early returns and make the main path obvious.
- Add an interface only for a real substitution boundary used by the current design. Keep one-use logic inline unless extracting it names a non-trivial rule or materially improves testing.
- Do not add configuration, retry, caching, concurrency, generic abstractions, or new dependencies without a requirement or measured need.
- Apply the repository's `AGENTS.md` for the Go target, naming, logging, error, and goroutine conventions. Use `go-code` for Go quality gates as required by the repository.
- Avoid silent retries, default substitution, swallowed errors, empty-value substitution, and best-effort continuation unless explicitly required and observable.

## Repo Routing

Use the requirement routing and module ownership in the repository's `docs/README.md`. Keep project paths and current implementation facts there instead of maintaining a second directory map in this skill.

## Testing

- Reuse existing coverage first. Add or update tests only to close a meaningful coverage gap for important changed behavior or a concrete regression risk. Low-risk changes with sufficient existing coverage need no new tests. Do not test implementation wording or structure.
- Prefer focused package tests for narrow changes. Broaden to `go test ./... -race -cover` when shared contracts or cross-layer behavior justify the cost.
- Use table-driven tests when several cases share meaningful setup; do not force a table for a single simple case.
- Run formatting and the smallest meaningful static or build checks for the touched surface. Preserve repository-required checks and use `go-code` for risk-specific validation. If a required or risk-justified check cannot run, state exactly what remains unverified.
- For bug fixes, including review-driven fixes, verify the actual failing scenario and corrected result using existing coverage or a focused reproduction; add a regression test when meaningful coverage is missing. Add adjacent cases only for a concrete uncovered regression risk.

## Docs And Devlogs

- Update docs when behavior, API, config, workflow contracts, manifests, or operational usage changes.
- Update repo `docs/README.md` when documents are added, moved, removed, or their navigation/status changes. Editing existing content alone does not require an index change.
- Add a `devlogs/` decision record only for a durable, non-obvious technical decision that future maintainers need in order to understand or safely change the system.
- Do not create docs or devlogs for internal refactors, ordinary bug fixes, tests, formatting, comments, or typo fixes when no contract or durable decision changed. Do not add an artifact merely to satisfy process.

## Definition Of Clean

A change is complete only when:

- the requested behavior and preserved contracts are both demonstrated;
- the diff is the smallest coherent diff, with no unnecessary entity or parallel mechanism;
- failure behavior is explicit and errors retain useful operation context;
- validation demonstrates the relevant behavior in proportion to risk, using existing coverage, focused checks, and new tests only where warranted;
- documentation matches any changed external or operational contract;
- formatting, focused checks, and final diff inspection are complete; and
- any unverified area or remaining risk is named precisely.

## PR Hygiene

- Update PR descriptions only when new commits change scope, user-visible behavior/API, configuration, or risk.
- Do not commit, push, publish, or change repository settings without explicit user authorization.

## References

- [docs/context-index.md](docs/context-index.md) - compact repo concept and context map.
- [docs/context-packet.md](docs/context-packet.md) - optional compact task brief for ambiguous or high-risk work.
