[English](./README.md) | [简体中文](./README_zh.md)

# Eruun

A distributed runtime for agents, models, and AI workloads.

Eruun is evolving from a Kubernetes workflow runtime into an open-source runtime for self-hosted AI workloads. Its implemented foundation already provides declarative applications, durable workflows, isolated workspaces, and distributed Kubernetes reconciliation. Agent tools, evaluation, vectorization, and model-serving capabilities remain proposals until the documentation index marks them Current.

## Capability maturity

| Stage | Scope | Status |
| --- | --- | --- |
| Current | Application and component lifecycle, traits, StepByStep and DAG workflows, API/controller/scheduler/worker roles, Redis or Kafka dispatch, MySQL execution leases, Kubernetes reconciliation, validation, status, logs, files, shell access, authentication, and workspace isolation | Implemented on `main` |
| Next | Containerized agent workloads, MCP and CLI tool bindings, credential and permission boundaries, audit evidence, and workflow-backed agent evaluation | Direction; contracts are not yet frozen |
| Later | Self-hosted model serving, GPU-aware placement, vectorization pipelines, managed AI provider integration, and broader cloud or multi-cluster execution | Exploration |

The roadmap deliberately starts with Kubernetes-hosted agents and their security boundary. It does not introduce a separate top-level Agent resource or promise speculative APIs. See [AI runtime vision](docs/ai-runtime-vision.md) for the intended capability layers and decision gates.

## Current runtime

Eruun currently provides:

- An OAM-inspired application model composed of components, traits, and workflows.
- REST APIs for application lifecycle, workflow execution, validation, status, logs, files, shell execution, settings, authentication, workspaces, and namespace adoption.
- StepByStep and DAG workflow execution backed by Redis Streams or Kafka.
- Separate `api`, `controller`, `scheduler`, and `worker` runtime roles with database leases and fencing.
- Kubernetes reconciliation for workloads, Services, storage, RBAC, ingress, probes, sidecars, init containers, rollout policies, and shared resources.
- Helm and standalone-manifest deployment paths.

Eruun ships only the server runtime. It does not include a client command-line application.

## Quick start

Prerequisites: Go 1.25, GNU Make, `kubectl`, Helm, and access to a Kubernetes cluster.

Configure the required account Secret from [accounts.example.json](deploy/accounts.example.json) before starting. Authentication, personal and team workspaces, HTTPS integration, and Kubernetes isolation requirements are documented in [Account and workspace integration](docs/account-auth-workspaces.md).

The installer generates MySQL and Redis credentials locally when they are not supplied. Generated values are held only in protected temporary files and Kubernetes Secrets.

```bash
AUTH_CONFIG_FILE=/secure/eruun/accounts.json SKIP_CONFIRM=true INSTALL_MODE=helm \
  ./deploy/all_in_one_install_quickstart.sh

kubectl -n eruun-system port-forward svc/eruun 8000:8000

curl --fail http://127.0.0.1:8000/api/v1/healthz
curl --fail http://127.0.0.1:8000/api/v1/readyz
```

For local development, start MySQL, Redis, and Kafka with the [local dependencies guide](docs/local-docker-dependencies.md), then run:

```bash
export ERUUN_DATASTORE_URL='eruun:__REPLACE_WITH_MYSQL_PASSWORD__@tcp(127.0.0.1:3306)/eruun?charset=utf8mb4&parseTime=true'
export ERUUN_CACHE_PASSWORD='__REPLACE_WITH_REDIS_PASSWORD__'
export ERUUN_AUTH_CONFIG_FILE='/secure/eruun/accounts.json'
go run ./cmd/main.go
```

The server listens on `127.0.0.1:8001` by default. Set `ERUUN_BIND_ADDR=0.0.0.0:8001` only when the local process must be reachable beyond localhost. The Kubernetes deployment paths explicitly override the listener to port `8000`.

## Architecture

The same server binary starts as exactly one role selected by `--role` or `ERUUN_ROLE`:

- `api` owns HTTP contracts, authentication, authorization, validation, and task creation.
- `controller` observes Kubernetes and projects runtime state back into the database.
- `scheduler` claims waiting workflow runs and publishes versioned dispatch messages.
- `worker` consumes dispatches and executes workflows and Kubernetes jobs.

MySQL is the durable source of truth for workflow state and execution ownership. Redis is required for cache, application-mutation locks, cancellation signaling, and the default Redis Streams transport. Kafka can replace Redis Streams for workflow messaging, but it does not replace MySQL ownership or Redis-backed application coordination.

## Documentation

- [Documentation index](docs/README.md) — status-aware navigation and current code facts.
- [AI runtime vision](docs/ai-runtime-vision.md) — target capability layers and roadmap; not an implemented contract.
- [Architecture overview](docs/架构文档.md) — current component, trait, workflow, and runtime boundaries.
- [Workflow architecture](docs/workflow-architecture-guide.md) — current workflow execution model.
- [Distributed runtime](docs/enterprise-distributed-runtime-design.md) — current roles, leases, fencing, and recovery.

## Development

```bash
make build
make test
go test ./... -race -cover
go vet ./...
```

## License

Eruun is licensed under the MIT License. See [LICENSE](LICENSE).
