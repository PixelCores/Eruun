# Repository Guidelines

Entities should not be multiplied unnecessarily.

## Product Scope

Eruun is “A distributed runtime for agents, models, and AI workloads.” The current implementation is a Go API server and Kubernetes workflow runtime. Documentation marked Draft or Proposal describes future direction and must not be presented as implemented behavior.

Eruun is an independent product line. Do not add compatibility aliases for predecessor names, environment variables, resource keys, images, or installation layouts.

## Structure and Requirement Routing

Start with `docs/README.md` before broad exploration.

- Server entrypoint: `cmd/main.go`.
- HTTP/API: `pkg/apiserver/interfaces/api`; DTOs in `dto/v1`; assemblers in `assembler/v1`.
- Business rules and lifecycle: `pkg/apiserver/domain/service`; models and repository contracts in adjacent domain packages.
- Workflow scheduling and execution: `pkg/apiserver/event/workflow`; Kubernetes jobs in `event/workflow/job`.
- Traits and naming: `pkg/apiserver/workflow/traits` and `pkg/apiserver/workflow/naming`.
- External adapters: `pkg/apiserver/infrastructure`.
- Deployment: `deploy/eruun-stack.yaml`, `deploy/helm/eruun`, and `deploy/all_in_one_install_quickstart.sh`.
- Tests are colocated as `*_test.go`; behavior/config changes require focused docs and an index update.

Eruun does not ship a client command-line application. API examples use `curl`, Helm, or Kubernetes tools.

## Build and Validation

- `go run ./cmd/main.go` — run the API server.
- `go build -o eruun-server ./cmd/main.go` — build the server binary.
- `make build` / `make build-linux` — build the default or Linux server.
- `go fmt ./...` — format Go packages.
- `go vet ./...` — run static analysis.
- `go test ./... -race -cover` — run the full Go suite.
- `deploy/all_in_one_install_quickstart_test.sh` — validate secure installer behavior.
- `deploy/helm/eruun/helm_template_test.sh` — validate rendered Helm contracts.

## Configuration and Naming

- Go module: `github.com/PixelCores/Eruun`.
- Server binary: `eruun-server`.
- Version symbol: `version.EruunVersion`.
- Server flags map to `ERUUN_` environment variables, for example `--bind-addr` to `ERUUN_BIND_ADDR`.
- Kubernetes metadata uses `eruun.io/*`; workflow API group uses `core.eruun.io`.
- Default image: `ghcr.io/pixelcores/eruun:0.1.0`.
- Preserve existing `/api/v1` routes and JSON field contracts unless a change explicitly updates that contract.

## Code and Test Style

- Target Go 1.25 and keep package paths lowercase.
- Use `k8s.io/klog/v2` structured logging for new logs.
- Wrap errors with operation context, such as `fmt.Errorf("create pvc: %w", err)`.
- Propagate `context.Context` into goroutines; prefer `errgroup` or `sync.WaitGroup`.
- Favor table-driven tests with edge and error cases.
- Keep business rules out of generic utility packages.

## Security and Open-Source Hygiene

- Never commit credentials, tokens, private repository references, internal domains, or personal information.
- Tracked Secret examples contain placeholders only. Helm must fail on empty or placeholder credentials.
- Installation scripts generate secrets locally, use mode `0600` temporary files, remove them on exit, and never print values.
- Use explicit image tags and resource requests/limits in deployment manifests.
- Treat third-party license reports as technical due-diligence input, not legal approval.

## Version and Delivery

- Behavior, API, configuration, workflow, or deployment changes require docs updates.
- Increment `EruunVersion`, Chart version, appVersion, and the default image tag together for a release change.
- Commits use `type: short description`. PR text states what/why, risk, and test evidence.
- Do not commit, push, publish images, create releases, or change repository settings without explicit authorization.

## Skill Trigger Discipline

- For Eruun implementation, review, refactoring, tests, builds, behavior/config docs, or delivery work, use `eruun-code`.
- For creating or updating skills, use `skill-creator`.
- Use `go-code` for Go quality gates and `codex-efficiency-protocol` for multi-step or publish-sensitive coordination.
- Announce selected skills before substantive work and include a `Skills used:` line in the final response.
