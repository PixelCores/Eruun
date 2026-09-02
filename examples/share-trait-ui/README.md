# Share Trait UI Examples

This folder provides frontend-friendly examples for testing `traits.share` behavior.

## Files

- `01-share-default.json`: two components for comparison (`strategy: default` vs no `share`)
- `02-share-ignore.json`: single component with `strategy: ignore`
- `03-share-force.json`: single component with `strategy: force`
- `04-share-mixed-ui.json`: one app containing all three strategies

## Suggested UI Test Steps

1. Import `04-share-mixed-ui.json` in the frontend and create the app.
2. Execute the workflow once.
3. Execute the same workflow again.

Note:

- These examples use `default` namespace by default.
- If you change to a custom namespace, create it first (for example: `kubectl create namespace <ns>`).

## Expected Job Status in UI

First execution:

- `cm-default`: `passed` / `completed`
- `cm-ignore`: `skipped`
- `cm-force`: `passed` / `completed`

Second execution:

- `cm-default`: `skipped`
- `cm-ignore`: `skipped`
- `cm-force`: `passed` / `completed`
