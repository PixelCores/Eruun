# Eruun Development Guide

Eruun is a distributed runtime for agents, models, and AI workloads. Its current implementation is a Go API server and Kubernetes workflow runtime; Draft and Proposal documents are future direction, not shipped capability.

Use `AGENTS.md` as the repository collaboration contract and `docs/README.md` as the architecture and document index.

## Common commands

```bash
go run ./cmd/main.go
go build -o eruun-server ./cmd/main.go
go fmt ./...
go vet ./...
go test ./... -race -cover
deploy/all_in_one_install_quickstart_test.sh
```

Server flags are exposed through `ERUUN_`-prefixed environment variables. The REST API remains under `/api/v1`. Eruun contains no client command-line application; use HTTP, Helm, and Kubernetes examples.

Keep API, domain, workflow, infrastructure, and deployment boundaries described in `AGENTS.md`. Never commit credentials or private/internal references.
