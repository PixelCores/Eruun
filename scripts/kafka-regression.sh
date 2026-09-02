#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER_NAME="eruun-kafka-it"
DEFAULT_BROKER="127.0.0.1:19092"

cleanup() {
  if [[ "${STARTED_CONTAINER:-0}" == "1" ]]; then
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ -z "${KAFKA_BROKER:-}" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required when KAFKA_BROKER is not provided" >&2
    exit 1
  fi

  STARTED_CONTAINER=1
  KAFKA_BROKER="${DEFAULT_BROKER}"

  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker run -d --name "${CONTAINER_NAME}" \
    -p 19092:9092 \
    docker.redpanda.com/redpandadata/redpanda:v24.1.6 \
    redpanda start \
      --overprovisioned \
      --smp 1 \
      --memory 1G \
      --reserve-memory 0M \
      --node-id 0 \
      --check=false \
      --kafka-addr PLAINTEXT://0.0.0.0:9092 \
      --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092 >/dev/null

  echo "waiting for redpanda to become ready..."
  for _ in {1..40}; do
    if docker exec "${CONTAINER_NAME}" rpk cluster info >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

export KAFKA_BROKER

echo "running Kafka integration regression with broker: ${KAFKA_BROKER}"
cd "${ROOT_DIR}"
go test -tags=integration ./pkg/apiserver/infrastructure/messaging -run KafkaIntegration -count=1 -v
