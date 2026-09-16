#!/usr/bin/env bash
set -euo pipefail

mode="${1:-generate}"
if [[ "$mode" != generate && "$mode" != check ]]; then
  echo "usage: $0 [generate|check]" >&2
  exit 2
fi

protoc_bin="${PROTOC:-protoc}"
go_plugin="${PROTOC_GEN_GO:-protoc-gen-go}"
grpc_plugin="${PROTOC_GEN_GO_GRPC:-protoc-gen-go-grpc}"
module="github.com/PixelCores/Eruun"

if [[ "$("$protoc_bin" --version)" != "libprotoc 35.0" ]]; then
  echo "gRPC generation requires protoc 35.0" >&2
  exit 1
fi
if [[ "$("$go_plugin" --version)" != "protoc-gen-go v1.36.12" ]]; then
  echo "gRPC generation requires protoc-gen-go v1.36.12" >&2
  exit 1
fi
if [[ "$("$grpc_plugin" --version)" != "protoc-gen-go-grpc 1.6.2" ]]; then
  echo "gRPC generation requires protoc-gen-go-grpc v1.6.2" >&2
  exit 1
fi

output="."
if [[ "$mode" == check ]]; then
  output="$(mktemp -d)"
  trap 'rm -r "$output"' EXIT
fi

"$protoc_bin" \
  --proto_path=proto \
  --plugin="protoc-gen-go=$go_plugin" \
  --plugin="protoc-gen-go-grpc=$grpc_plugin" \
  --go_out="$output" --go_opt="module=$module" \
  --go-grpc_out="$output" --go-grpc_opt="module=$module" \
  proto/eruun/v1/account.proto \
  proto/eruun/v1/administration.proto \
  proto/eruun/v1/applications.proto \
  proto/eruun/v1/jobs.proto \
  proto/eruun/v1/resource_import.proto

if [[ "$mode" == check ]]; then
  for generated in "$output"/pkg/apiserver/interfaces/grpc/pb/v1/*.pb.go; do
    tracked="pkg/apiserver/interfaces/grpc/pb/v1/${generated##*/}"
    if ! cmp -s "$generated" "$tracked"; then
      diff -u "$tracked" "$generated" || true
      echo "generated gRPC file differs: $tracked" >&2
      exit 1
    fi
  done
  echo "gRPC generated code matches committed files"
fi
