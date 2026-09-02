# App Workflow Callback Contract

## Context

Workflow callback configuration was stored only on `Workflow`. App creation now needs an App-level callback while still allowing custom workflow creation to override it.

## Decision

- Add `Applications.Callback` as the App-level callback record.
- Keep `Workflow.Callback` as the execution-time effective callback.
- Accept both legacy `workflow: [...]` and new `workflow: {callback, steps}` App create payloads.
- On App update with root `callback`, overwrite all workflow callbacks for the App.
- During execution, read `workflow.callback` first and fall back to `app.callback` for legacy or missing workflow data.

## Risks

- The `applications` table receives a new JSON column through AutoMigrate.
- App update with `callback: {}` intentionally clears callbacks for all workflows in that App.
