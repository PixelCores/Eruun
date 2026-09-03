#!/usr/bin/env bash

set -Eeuo pipefail
umask 077

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"

require_credential() {
  local variable_name="$1"
  local value="$2"
  if [ -z "${value}" ]; then
    printf 'Set %s to a non-placeholder value before starting local dependencies.\n' "${variable_name}" >&2
    exit 1
  fi
  case "${value}" in
    *__REPLACE_*)
      printf '%s contains unresolved placeholder text.\n' "${variable_name}" >&2
      exit 1
      ;;
  esac
}

command -v docker >/dev/null 2>&1 || {
  printf '%s\n' 'docker not found in PATH' >&2
  exit 1
}
require_credential MYSQL_ROOT_PASSWORD "${MYSQL_ROOT_PASSWORD}"
require_credential MYSQL_PASSWORD "${MYSQL_PASSWORD}"
require_credential REDIS_PASSWORD "${REDIS_PASSWORD}"
export MYSQL_ROOT_PASSWORD MYSQL_PASSWORD REDIS_PASSWORD

docker compose version >/dev/null
printf '%s\n' 'Starting local MySQL, Redis and Kafka dependencies for Eruun...'
docker compose -f "${SCRIPT_DIR}/compose.yaml" up -d --wait --wait-timeout 180

printf 'MySQL: 127.0.0.1:%s, database=%s, user=eruun\n' "${MYSQL_PORT:-3306}" "${MYSQL_DATABASE:-eruun}"
printf 'Redis: 127.0.0.1:%s\n' "${REDIS_PORT:-6379}"
printf 'Kafka: localhost:%s (host), kafka:19092 (Compose network)\n' "${KAFKA_PORT:-9092}"
printf '%s\n' 'Credentials are mounted as Compose secrets and are intentionally not printed.'
printf '%s\n' 'Existing containers and data volumes are preserved. See docs/local-docker-dependencies.md for server configuration and stop commands.'
