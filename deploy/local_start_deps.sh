#!/usr/bin/env bash

set -Eeuo pipefail
umask 077

MYSQL_CONTAINER_NAME="${MYSQL_CONTAINER_NAME:-eruun-mysql}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-}"
MYSQL_DATABASE="${MYSQL_DATABASE:-eruun}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"

REDIS_CONTAINER_NAME="${REDIS_CONTAINER_NAME:-eruun-redis}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7}"

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
require_credential REDIS_PASSWORD "${REDIS_PASSWORD}"

remove_if_exists() {
  local name="$1"
  if docker ps -a --format '{{.Names}}' | grep -Fxq "$name"; then
    docker rm -f "$name" >/dev/null
  fi
}

echo "Starting local MySQL and Redis dependencies for Eruun..."

remove_if_exists "$MYSQL_CONTAINER_NAME"
remove_if_exists "$REDIS_CONTAINER_NAME"

docker run -d \
  --name "$MYSQL_CONTAINER_NAME" \
  -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
  -e MYSQL_DATABASE="$MYSQL_DATABASE" \
  -p "$MYSQL_PORT:3306" \
  "$MYSQL_IMAGE"

docker run -d \
  --name "$REDIS_CONTAINER_NAME" \
  -e REDIS_PASSWORD="$REDIS_PASSWORD" \
  -p "$REDIS_PORT:6379" \
  "$REDIS_IMAGE" \
  /bin/sh -ec 'exec redis-server --save 60 1 --appendonly yes --requirepass "$REDIS_PASSWORD"'

cat <<EOF
MySQL:
  container: $MYSQL_CONTAINER_NAME
  port:      $MYSQL_PORT
  database:  $MYSQL_DATABASE

Redis:
  container: $REDIS_CONTAINER_NAME
  port:      $REDIS_PORT
EOF

printf '%s\n' 'Credentials were read from MYSQL_ROOT_PASSWORD and REDIS_PASSWORD and are intentionally not printed.'
printf '%s\n' 'Configure ERUUN_DATASTORE_URL and ERUUN_CACHE_PASSWORD with the same values before starting the server.'
