# Contributing to Eruun

Thank you for helping improve Eruun.

## Before you start

- Read `AGENTS.md` and `docs/README.md`.
- Use an issue or draft pull request for changes that alter public APIs, workflow semantics, persisted data, or deployment topology.
- Do not submit confidential code, credentials, private infrastructure details, or material you are not authorized to license.
- By contributing, you represent that you have the right to submit the contribution under the repository's MIT License.

## Development

Eruun targets Go 1.25. Keep changes focused and follow the existing API, domain, workflow, infrastructure, and deployment boundaries.

```bash
go fmt ./...
go vet ./...
go test ./... -race -cover
go build -o eruun-server ./cmd/main.go
deploy/all_in_one_install_quickstart_test.sh
```

For deployment changes, also run the manifest tests and Helm lint/template checks described in `AGENTS.md`.

## Continuous integration

GitHub Actions runs Go formatting, vet, race tests, server builds, deployment and Helm checks, container builds, and sensitive-content scanning. Dependency Review checks newly introduced dependency vulnerabilities at moderate severity or above; its license checks are disabled.

The repository does not maintain a generated dependency-license inventory, manual license overrides, or a separate public-launch checklist. The MIT License and contribution requirements above remain unchanged.

Run `scripts/check-sensitive-content.sh` locally to check for sensitive content and obsolete product references.

## Documentation

- Mark shipped behavior as Current only when it is verified against code and tests.
- Keep future product ideas as Draft / Proposal.
- Update `docs/README.md` when adding or removing a focused document.
- Use HTTP, Helm, or Kubernetes examples. Eruun does not ship a client command-line application.

## Pull requests

Include:

- what changed and why;
- user-visible behavior and compatibility impact;
- security and operational risks;
- test commands and results;
- documentation changes;
- version changes when preparing a release.

Keep commits focused and use `type: short description`, such as `fix: reject placeholder credentials`.

## Security

Do not open a public issue for a suspected vulnerability. Follow `SECURITY.md`.
