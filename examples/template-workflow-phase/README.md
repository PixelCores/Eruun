# Template Workflow Phase Examples

These examples show how template instantiation behaves after the workflow staging change.

## Files

- `create-app-auto-staged.json`
  - Uses one `tmp.id` to reference multiple template components.
  - Does not provide `workflow`, so the server generates the default staged workflow.
- `create-app-explicit-workflow.json`
  - Also uses `tmp.id`, but provides explicit `workflow`.
  - The server should keep the provided workflow and not rewrite it.

## API

`POST /api/v1/applications`

## Quick test

```bash
curl -sS -X POST "http://127.0.0.1:8000/api/v1/applications" \
  -H "Content-Type: application/json" \
  -d @examples/template-workflow-phase/create-app-auto-staged.json
```

```bash
curl -sS -X POST "http://127.0.0.1:8000/api/v1/applications" \
  -H "Content-Type: application/json" \
  -d @examples/template-workflow-phase/create-app-explicit-workflow.json
```

## Preconditions

- The template application ID in `tmp.id` exists.
- The template application has `templateEnabled=true`.
- The target names in `tmp.target` exist in the template components.

