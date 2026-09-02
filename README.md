[English](./README.md) | [简体中文](./README_zh.md)

# Eruun

A distributed runtime for agents, models, and AI workloads.

Eruun is an open-source Kubernetes application runtime built around declarative components, traits, and durable workflows. The current release provides the API server, distributed controller/scheduler/worker roles, Kubernetes reconciliation, lifecycle workflows, validation, status, and operational APIs.

Agent evaluation, vectorization, and distributed model-serving documents are design proposals unless they are explicitly marked Current in the documentation index.

## What Eruun provides

- An OAM-inspired application model composed of components, traits, and workflows.
- REST APIs for application lifecycle, workflow execution, validation, status, logs, files, shell execution, settings, and namespace adoption.
- StepByStep and DAG workflow execution backed by Redis Streams or Kafka.
- Dedicated API, controller, scheduler, and worker runtime roles with database leases and fencing.
- Kubernetes reconciliation for workloads, Services, storage, RBAC, ingress, probes, sidecars, init containers, rollout policies, and shared resources.
- Helm and standalone-manifest deployment paths.

Eruun ships only the server runtime. It does not include a client command-line application.

## Quick start

Prerequisites: Go 1.25, GNU Make, kubectl, Helm, and access to a Kubernetes cluster.

The installer generates MySQL and Redis credentials locally when they are not supplied. Generated values are held only in protected temporary files and Kubernetes Secrets.

~~~bash
SKIP_CONFIRM=true INSTALL_MODE=helm \
  ./deploy/all_in_one_install_quickstart.sh

kubectl -n eruun-system port-forward svc/eruun 8000:8000

curl --fail http://127.0.0.1:8000/api/v1/healthz
curl --fail http://127.0.0.1:8000/api/v1/readyz
~~~

To run the server locally:

~~~bash
export ERUUN_DATASTORE_URL='eruun:__REPLACE_WITH_MYSQL_PASSWORD__@tcp(127.0.0.1:3306)/eruun?charset=utf8mb4&parseTime=true'
export ERUUN_CACHE_PASSWORD='__REPLACE_WITH_REDIS_PASSWORD__'
go run ./cmd/main.go
~~~

The server listens on 127.0.0.1:8000 by default. Set ERUUN_BIND_ADDR=0.0.0.0:8000 only when the listener must be reachable beyond localhost.

## Architecture

Eruun can run all responsibilities in one process or split them across four roles:

- api: HTTP routes, request validation, and response contracts.
- controller: Kubernetes observation and runtime-state synchronization.
- scheduler: workflow scheduling and dispatch.
- worker: workflow jobs and Kubernetes resource reconciliation.
- all: the default single-process development mode.

MySQL is the durable state store and the authoritative owner of workflow execution leases. Redis is required for distributed application-mutation locks and is the default Redis Streams message transport. Kafka can be selected for workflow messaging while Redis remains the cache and application-lock backend; workflow workers do not take a second Redis execution lock.

## Configuration

Every server flag has an ERUUN_ environment-variable equivalent generated from its flag name. Common settings include:

- ERUUN_BIND_ADDR
- ERUUN_DATASTORE_URL
- ERUUN_CACHE_HOST, ERUUN_CACHE_PORT, and ERUUN_CACHE_PASSWORD
- ERUUN_MSG_TYPE and ERUUN_MSG_KAFKA_BROKERS
- ERUUN_ROLE

Run the following command for the complete server contract:

~~~bash
go run ./cmd/main.go --help
~~~

Default configuration is documented in config/apiserver-default.yaml.

## Development

~~~bash
make build
make test
go test ./... -race -cover
go vet ./...
~~~

Key documentation:

- docs/README.md — status-aware documentation index
- docs/workflow-architecture-guide.md — workflow engine architecture
- docs/enterprise-distributed-runtime-design.md — distributed runtime roles and leases
- docs/core-module-boundary-and-cross-layer-contracts.md — API, domain, persistence, and Kubernetes boundaries
- examples/ — HTTP request payloads and operational examples

## Security

Please read SECURITY.md before reporting a vulnerability.

## License

Eruun is licensed under the MIT License. See LICENSE.
