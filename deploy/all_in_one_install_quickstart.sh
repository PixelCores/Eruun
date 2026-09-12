#!/usr/bin/env bash

set -Eeuo pipefail
umask 077

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LOGPATH="${SCRIPT_DIR}/installer.log"

INSTALL_MODE=$(printf '%s' "${INSTALL_MODE:-manifest}" | tr '[:upper:]' '[:lower:]')
NAMESPACE="${NAMESPACE:-eruun-system}"
MANIFEST="${MANIFEST:-${SCRIPT_DIR}/eruun-stack.yaml}"
CHART_PATH="${CHART_PATH:-${SCRIPT_DIR}/helm/eruun}"
RELEASE_NAME="${RELEASE_NAME:-eruun}"
FULLNAME_OVERRIDE="${FULLNAME_OVERRIDE-eruun}"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
HELM_BIN="${HELM_BIN:-helm}"
OPENSSL_BIN="${OPENSSL_BIN:-openssl}"
SKIP_CONFIRM="${SKIP_CONFIRM:-false}"
DRY_RUN="${DRY_RUN:-false}"
WAIT_READY="${WAIT_READY:-true}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
ENABLE_PORT_FORWARD="${ENABLE_PORT_FORWARD:-false}"
LOCAL_PORT="${LOCAL_PORT:-8000}"
REMOTE_PORT="${REMOTE_PORT:-8000}"
SERVICE_NAME="${SERVICE_NAME-}"
SERVICE_TYPE="${SERVICE_TYPE:-}"
SERVICE_PORT="${SERVICE_PORT:-8000}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-eruun}"
CONTAINER_NAME="${CONTAINER_NAME:-eruun-server}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-}"
IMAGE_TAG="${IMAGE_TAG:-}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-}"
REPLICA_COUNT="${REPLICA_COUNT:-}"
MYSQL_IMAGE="${MYSQL_IMAGE:-}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-}"
MYSQL_USER_INPUT_SET="${MYSQL_USER+x}"
MYSQL_USER="${MYSQL_USER:-eruun}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_DATABASE_INPUT_SET="${MYSQL_DATABASE+x}"
MYSQL_DATABASE="${MYSQL_DATABASE:-eruun}"
MYSQL_STORAGE="${MYSQL_STORAGE:-}"
MYSQL_SERVICE_PORT="${MYSQL_SERVICE_PORT:-}"
REDIS_IMAGE="${REDIS_IMAGE:-}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_STORAGE="${REDIS_STORAGE:-}"
REDIS_SERVICE_PORT="${REDIS_SERVICE_PORT:-}"
REDIS_WORKLOAD_KIND=$(printf '%s' "${REDIS_WORKLOAD_KIND:-statefulset}" | tr '[:upper:]' '[:lower:]')
REDIS_WORKLOAD_NAME="${REDIS_WORKLOAD_NAME:-eruun-redis}"

AUTH_CONFIG_FILE="${AUTH_CONFIG_FILE:-}"

TEMP_FILES=()
PORT_FORWARD_PID=

logInfo() {
  printf '[INFO] %s\n' "$1" >&2
  printf '[INFO] %s\n' "$1" >> "${LOGPATH}"
}

logWarn() {
  printf '[WARN] %s\n' "$1" >&2
  printf '[WARN] %s\n' "$1" >> "${LOGPATH}"
}

logSuccess() {
  printf '[SUCCESS] %s\n' "$1" >&2
  printf '[SUCCESS] %s\n' "$1" >> "${LOGPATH}"
}

bail() {
  printf '[ERROR] %s\n' "$1" >&2
  printf '[ERROR] %s\n' "$1" >> "${LOGPATH}"
  exit 1
}

isTrue() {
  case "$1" in
    1|true|TRUE|True|yes|YES|Yes|y|Y|on|ON|On) return 0 ;;
    *) return 1 ;;
  esac
}

runCmd() {
  logInfo "RUN: $*"
  "$@" 2>&1 | tee -a "${LOGPATH}"
}

registerTempFile() {
  TEMP_FILES+=("$1")
}

cleanup() {
  local file
  for file in "${TEMP_FILES[@]:-}"; do
    if [ -n "${file}" ] && [ -f "${file}" ]; then
      rm -f "${file}" || true
    fi
  done
}

onExit() {
  local code=$?
  cleanup
  if [ "${code}" -ne 0 ]; then
    logWarn "Eruun installation failed"
  fi
  return "${code}"
}

trap onExit EXIT

checkCommand() {
  command -v "$1" >/dev/null 2>&1 || bail "required command not found: $1"
}

generatePassword() {
  "${OPENSSL_BIN}" rand -hex 24
}

ensureCredential() {
  local variable_name="$1"
  local current_value="$2"

  case "${current_value}" in
    *__REPLACE_*) bail "${variable_name} contains unresolved placeholder text" ;;
  esac
  if [ -n "${current_value}" ]; then
    printf '%s' "${current_value}"
    return 0
  fi

  logInfo "Generating ${variable_name} locally"
  generatePassword
}

resourceExists() {
  local kind="$1"
  local name="$2"
  local output
  if output=$("${KUBECTL_BIN}" -n "${NAMESPACE}" get "${kind}" "${name}" -o name 2>&1); then
    return 0
  fi
  case "${output}" in
    *NotFound*|*"not found"*) return 1 ;;
    *) bail "cannot inspect existing ${kind} ${name}" ;;
  esac
}

readSecretValue() {
  local secret_name="$1"
  local key="$2"
  local workload_kind="$3"
  local workload_name="$4"
  local pvc_name="$5"
  local allow_missing_key="${6:-false}"
  local encoded
  if ! encoded=$("${KUBECTL_BIN}" -n "${NAMESPACE}" get secret "${secret_name}" -o "go-template={{ index .data \"${key}\" }}" 2>&1); then
    case "${encoded}" in
      *NotFound*|*"not found"*)
        if resourceExists "${workload_kind}" "${workload_name}" || resourceExists persistentvolumeclaim "${pvc_name}"; then
          bail "credential Secret ${secret_name} is missing while persistent resources still exist"
        fi
        return 0
        ;;
      *) bail "cannot read existing Secret ${secret_name}" ;;
    esac
  fi
  case "${encoded}" in
    ""|"<no value>")
      if isTrue "${allow_missing_key}"; then
        return 0
      fi
      bail "existing Secret ${secret_name} is missing required key ${key}"
      ;;
  esac
  printf '%s' "${encoded}" | "${OPENSSL_BIN}" base64 -d -A 2>/dev/null || bail "cannot decode existing ${secret_name}/${key}"
}

readStatefulSetEnvValue() {
  local workload_name="$1"
  local container_name="$2"
  local env_name="$3"
  local value
  if ! value=$("${KUBECTL_BIN}" -n "${NAMESPACE}" get statefulset "${workload_name}" -o "jsonpath={.spec.template.spec.containers[?(@.name=='${container_name}')].env[?(@.name=='${env_name}')].value}" 2>&1); then
    bail "cannot recover ${env_name} from existing StatefulSet ${workload_name}"
  fi
  [ -n "${value}" ] || bail "existing StatefulSet ${workload_name} is missing required ${env_name} value"
  printf '%s' "${value}"
}

resolvePersistentCredential() {
  local variable_name="$1"
  local supplied="$2"
  local existing="$3"
  if [ -z "${existing}" ]; then
    ensureCredential "${variable_name}" "${supplied}"
    return
  fi
  if [ -n "${supplied}" ] && [ "${supplied}" != "${existing}" ]; then
    bail "${variable_name} differs from the existing persistent installation; use a separate credential rotation procedure"
  fi
  printf '%s' "${existing}"
}

resolvePersistentSetting() {
  local variable_name="$1"
  local supplied_flag="$2"
  local supplied="$3"
  local existing="$4"
  if [ -z "${existing}" ]; then
    printf '%s' "${supplied}"
    return
  fi
  if [ -n "${supplied_flag}" ] && [ "${supplied}" != "${existing}" ]; then
    bail "${variable_name} differs from the existing persistent installation; migrate the bundled database before changing it"
  fi
  printf '%s' "${existing}"
}

helmBaseName() {
  local name
  if [ -n "${FULLNAME_OVERRIDE}" ]; then
    name="${FULLNAME_OVERRIDE}"
  else
    name="${RELEASE_NAME}-eruun"
  fi
  name="${name:0:63}"
  printf '%s' "${name%-}"
}

helmSuffixedName() {
  local suffix="$1"
  local name max_base_length
  name=$(helmBaseName)
  max_base_length=$((62 - ${#suffix}))
  name="${name:0:${max_base_length}}"
  name="${name%-}"
  printf '%s-%s' "${name}" "${suffix}"
}

reusePersistentCredentials() {
  local mysql_secret redis_secret
  local mysql_workload redis_workload mysql_pvc redis_pvc
  local existing_mysql_root existing_mysql_user existing_mysql_password existing_mysql_database existing_redis_password
  if [ "${INSTALL_MODE}" = "helm" ]; then
    mysql_secret=$(helmSuffixedName mysql)
    redis_secret=$(helmSuffixedName redis)
    mysql_workload="${mysql_secret}"
    redis_workload="${redis_secret}"
    mysql_pvc="data-${mysql_workload}-0"
    redis_pvc="data-${redis_workload}-0"
    existing_mysql_root=$(readSecretValue "${mysql_secret}" password statefulset "${mysql_workload}" "${mysql_pvc}")
    existing_mysql_database=$(readSecretValue "${mysql_secret}" database statefulset "${mysql_workload}" "${mysql_pvc}" true)
    if [ -n "${existing_mysql_root}" ] && [ -z "${existing_mysql_database}" ]; then
      existing_mysql_database=$(readStatefulSetEnvValue "${mysql_workload}" mysql MYSQL_DATABASE)
    fi
  else
    mysql_secret="eruun-mysql-secret"
    redis_secret="eruun-secret"
    mysql_workload="eruun-mysql"
    redis_workload="eruun-redis"
    mysql_pvc="data-eruun-mysql-0"
    redis_pvc="data-eruun-redis-0"
    existing_mysql_root=$(readSecretValue "${mysql_secret}" mysql-root-password statefulset "${mysql_workload}" "${mysql_pvc}")
    existing_mysql_user=$(readSecretValue "${mysql_secret}" mysql-user statefulset "${mysql_workload}" "${mysql_pvc}")
    existing_mysql_password=$(readSecretValue "${mysql_secret}" mysql-password statefulset "${mysql_workload}" "${mysql_pvc}")
    existing_mysql_database=$(readSecretValue "${mysql_secret}" mysql-database statefulset "${mysql_workload}" "${mysql_pvc}")
  fi
  if [ "${INSTALL_MODE}" = "helm" ]; then
    existing_redis_password=$(readSecretValue "${redis_secret}" password statefulset "${redis_workload}" "${redis_pvc}")
  else
    existing_redis_password=$(readSecretValue "${redis_secret}" cache-password statefulset "${redis_workload}" "${redis_pvc}")
  fi

  MYSQL_ROOT_PASSWORD=$(resolvePersistentCredential MYSQL_ROOT_PASSWORD "${MYSQL_ROOT_PASSWORD}" "${existing_mysql_root}")
  REDIS_PASSWORD=$(resolvePersistentCredential REDIS_PASSWORD "${REDIS_PASSWORD}" "${existing_redis_password}")
  MYSQL_DATABASE=$(resolvePersistentSetting MYSQL_DATABASE "${MYSQL_DATABASE_INPUT_SET}" "${MYSQL_DATABASE}" "${existing_mysql_database}")
  if [ "${INSTALL_MODE}" = "manifest" ]; then
    MYSQL_USER=$(resolvePersistentSetting MYSQL_USER "${MYSQL_USER_INPUT_SET}" "${MYSQL_USER}" "${existing_mysql_user}")
    MYSQL_PASSWORD=$(resolvePersistentCredential MYSQL_PASSWORD "${MYSQL_PASSWORD}" "${existing_mysql_password}")
  fi
}

confirmInstall() {
  if isTrue "${SKIP_CONFIRM}" || [ ! -t 0 ]; then
    return 0
  fi

  local answer
  read -r -p "Install Eruun into namespace ${NAMESPACE}? [Y/n]: " answer
  case "${answer}" in
    n|N) bail "installation cancelled" ;;
  esac
}

resolveServiceName() {
  if [ -n "${SERVICE_NAME}" ]; then
    return 0
  fi
  if [ "${INSTALL_MODE}" = "helm" ]; then
    SERVICE_NAME=$(helmBaseName)
  else
    SERVICE_NAME="eruun"
  fi
}

preflightCheck() {
  [ -n "${AUTH_CONFIG_FILE}" ] && [ -s "${AUTH_CONFIG_FILE}" ] || bail "AUTH_CONFIG_FILE must name a nonempty account configuration file; see docs/account-auth-workspaces.md"

  case "${INSTALL_MODE}" in
    manifest|helm) ;;
    *) bail "unsupported INSTALL_MODE=${INSTALL_MODE}; expected manifest or helm" ;;
  esac
  case "${REDIS_WORKLOAD_KIND}" in
    deployment|statefulset) ;;
    *) bail "unsupported REDIS_WORKLOAD_KIND=${REDIS_WORKLOAD_KIND}" ;;
  esac
  case "${SERVICE_TYPE}" in
    ""|ClusterIP|NodePort|LoadBalancer) ;;
    *) bail "unsupported SERVICE_TYPE=${SERVICE_TYPE}" ;;
  esac
  case "${LOCAL_PORT}:${REMOTE_PORT}:${SERVICE_PORT}" in
    *[!0-9:]*|::*|:*:|*:) bail "service ports must be numeric" ;;
  esac
  if [ -n "${IMAGE_TAG}" ] && [ -z "${IMAGE_REPOSITORY}" ]; then
    bail "IMAGE_TAG requires IMAGE_REPOSITORY"
  fi

  checkCommand "${KUBECTL_BIN}"
  checkCommand "${OPENSSL_BIN}"
  if [ "${INSTALL_MODE}" = "helm" ]; then
    checkCommand "${HELM_BIN}"
    [ -f "${CHART_PATH}/Chart.yaml" ] || bail "Helm chart not found: ${CHART_PATH}"
  else
    [ -f "${MANIFEST}" ] || bail "manifest not found: ${MANIFEST}"
  fi

  "${KUBECTL_BIN}" cluster-info >/dev/null 2>&1 || bail "Kubernetes cluster is not reachable"

  reusePersistentCredentials

  resolveServiceName
}

ensureNamespace() {
  if "${KUBECTL_BIN}" get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    return 0
  fi
  if isTrue "${DRY_RUN}"; then
    logInfo "[DRY_RUN] namespace would be created: ${NAMESPACE}"
    return 0
  fi
  runCmd "${KUBECTL_BIN}" create namespace "${NAMESPACE}"
}

newTempYaml() {
  local prefix="$1"
  NEW_TEMP_YAML=$(mktemp "${TMPDIR:-/tmp}/${prefix}.XXXXXX.yaml")
  chmod 600 "${NEW_TEMP_YAML}"
  registerTempFile "${NEW_TEMP_YAML}"
}

createAccountSecret() {
  local auth_secret_file
  newTempYaml eruun-account-secret
  auth_secret_file="${NEW_TEMP_YAML}"
  "${KUBECTL_BIN}" -n "${NAMESPACE}" create secret generic eruun-account-config \
    --from-file=accounts.json="${AUTH_CONFIG_FILE}" --dry-run=client -o yaml > "${auth_secret_file}"
  if isTrue "${DRY_RUN}"; then
    runCmd "${KUBECTL_BIN}" apply --dry-run=client -f "${auth_secret_file}"
  else
    runCmd "${KUBECTL_BIN}" apply -f "${auth_secret_file}"
  fi
}

createManifestSecrets() {
  local mysql_secret_file
  local runtime_secret_file
  local datastore_url

  newTempYaml eruun-mysql-secret
  mysql_secret_file="${NEW_TEMP_YAML}"
  newTempYaml eruun-runtime-secret
  runtime_secret_file="${NEW_TEMP_YAML}"
  datastore_url="${MYSQL_USER}:${MYSQL_PASSWORD}@tcp(eruun-mysql:3306)/${MYSQL_DATABASE}?charset=utf8mb4&parseTime=true&loc=Local"

  "${KUBECTL_BIN}" -n "${NAMESPACE}" create secret generic eruun-mysql-secret \
    --from-literal=mysql-root-password="${MYSQL_ROOT_PASSWORD}" \
    --from-literal=mysql-user="${MYSQL_USER}" \
    --from-literal=mysql-password="${MYSQL_PASSWORD}" \
    --from-literal=mysql-database="${MYSQL_DATABASE}" \
    --dry-run=client -o yaml > "${mysql_secret_file}"

  "${KUBECTL_BIN}" -n "${NAMESPACE}" create secret generic eruun-secret \
    --from-literal=datastore-url="${datastore_url}" \
    --from-literal=cache-password="${REDIS_PASSWORD}" \
    --dry-run=client -o yaml > "${runtime_secret_file}"

  if isTrue "${DRY_RUN}"; then
    runCmd "${KUBECTL_BIN}" apply --dry-run=client -f "${mysql_secret_file}"
    runCmd "${KUBECTL_BIN}" apply --dry-run=client -f "${runtime_secret_file}"
  else
    runCmd "${KUBECTL_BIN}" apply -f "${mysql_secret_file}"
    runCmd "${KUBECTL_BIN}" apply -f "${runtime_secret_file}"
  fi
}

patchManifestOverrides() {
  if isTrue "${DRY_RUN}"; then
    return 0
  fi

  local role
  if [ -n "${IMAGE_REPOSITORY}" ]; then
    local image="${IMAGE_REPOSITORY}"
    if [ -n "${IMAGE_TAG}" ]; then
      image="${IMAGE_REPOSITORY}:${IMAGE_TAG}"
    fi
    for role in api controller scheduler worker; do
      runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" set image "deployment/${DEPLOYMENT_NAME}-${role}" "${CONTAINER_NAME}=${image}"
    done
  fi
  if [ -n "${REPLICA_COUNT}" ]; then
    runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" scale "deployment/${DEPLOYMENT_NAME}-api" --replicas="${REPLICA_COUNT}"
  fi
  if [ -n "${SERVICE_TYPE}" ]; then
    runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" patch "service/${SERVICE_NAME}" --type=merge -p "{\"spec\":{\"type\":\"${SERVICE_TYPE}\"}}"
  fi
}

installManifest() {
  ensureNamespace
  createAccountSecret
  createManifestSecrets

  if isTrue "${DRY_RUN}"; then
    runCmd "${KUBECTL_BIN}" apply --dry-run=client -f "${MANIFEST}"
    return 0
  fi

  runCmd "${KUBECTL_BIN}" apply --dry-run=server -f "${MANIFEST}"
  runCmd "${KUBECTL_BIN}" apply -f "${MANIFEST}"
  patchManifestOverrides
}

createHelmValues() {
  newTempYaml eruun-helm-sensitive-values
  HELM_VALUES_PATH="${NEW_TEMP_YAML}"
  {
    printf 'mysql:\n'
    printf '  rootPassword: %s\n' "${MYSQL_ROOT_PASSWORD}"
    printf 'redis:\n'
    printf '  password: %s\n' "${REDIS_PASSWORD}"
  } > "${HELM_VALUES_PATH}"
}

installHelm() {
  ensureNamespace
  createAccountSecret
  local sensitive_values
  createHelmValues
  sensitive_values="${HELM_VALUES_PATH}"
  local args=(
    upgrade --install "${RELEASE_NAME}" "${CHART_PATH}"
    --namespace "${NAMESPACE}" --create-namespace
    --set "fullnameOverride=${FULLNAME_OVERRIDE}"
    --set-string auth.existingSecret=eruun-account-config
    --values "${sensitive_values}"
  )

  if isTrue "${DRY_RUN}"; then args+=(--dry-run); fi
  if [ -n "${IMAGE_REPOSITORY}" ]; then args+=(--set "image.repository=${IMAGE_REPOSITORY}"); fi
  if [ -n "${IMAGE_TAG}" ]; then args+=(--set "image.tag=${IMAGE_TAG}"); fi
  if [ -n "${IMAGE_PULL_POLICY}" ]; then args+=(--set "image.pullPolicy=${IMAGE_PULL_POLICY}"); fi
  if [ -n "${SERVICE_TYPE}" ]; then args+=(--set "service.type=${SERVICE_TYPE}"); fi
  if [ -n "${SERVICE_PORT}" ]; then args+=(--set "service.port=${SERVICE_PORT}"); fi
  if [ -n "${REPLICA_COUNT}" ]; then args+=(--set "runtime.roles.api.replicas=${REPLICA_COUNT}"); fi
  if [ -n "${MYSQL_IMAGE}" ]; then args+=(--set "mysql.image=${MYSQL_IMAGE}"); fi
  if [ -n "${MYSQL_DATABASE}" ]; then args+=(--set "mysql.database=${MYSQL_DATABASE}"); fi
  if [ -n "${MYSQL_STORAGE}" ]; then args+=(--set "mysql.storage=${MYSQL_STORAGE}"); fi
  if [ -n "${MYSQL_SERVICE_PORT}" ]; then args+=(--set "mysql.servicePort=${MYSQL_SERVICE_PORT}"); fi
  if [ -n "${REDIS_IMAGE}" ]; then args+=(--set "redis.image=${REDIS_IMAGE}"); fi
  if [ -n "${REDIS_STORAGE}" ]; then args+=(--set "redis.storage=${REDIS_STORAGE}"); fi
  if [ -n "${REDIS_SERVICE_PORT}" ]; then args+=(--set "redis.servicePort=${REDIS_SERVICE_PORT}"); fi

  runCmd "${HELM_BIN}" "${args[@]}"
}

removeLegacyClusterAdminBinding() {
  # Only the bundled static manifest replaces the fixed legacy binding's subjects.
  if [ "${INSTALL_MODE}" != "manifest" ] || [ "${NAMESPACE}" != "eruun-system" ] || \
    ! [ "${MANIFEST}" -ef "${SCRIPT_DIR}/eruun-stack.yaml" ]; then
    return 0
  fi
  local args=(delete clusterrolebinding eruun-platform-cluster-admin --ignore-not-found=true)
  if isTrue "${DRY_RUN}"; then
    args+=(--dry-run=client)
  fi
  runCmd "${KUBECTL_BIN}" "${args[@]}"
}

waitForReady() {
  if isTrue "${DRY_RUN}" || ! isTrue "${WAIT_READY}"; then
    return 0
  fi

  local role
  if [ "${INSTALL_MODE}" = "helm" ]; then
    for role in api controller scheduler worker; do
      runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" rollout status "deployment/$(helmSuffixedName "${role}")" --timeout="${WAIT_TIMEOUT}"
    done
    runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" rollout status "statefulset/$(helmSuffixedName mysql)" --timeout="${WAIT_TIMEOUT}"
    runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" rollout status "statefulset/$(helmSuffixedName redis)" --timeout="${WAIT_TIMEOUT}"
    return 0
  fi
  for role in api controller scheduler worker; do
    runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" rollout status "deployment/${DEPLOYMENT_NAME}-${role}" --timeout="${WAIT_TIMEOUT}"
  done
  runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" rollout status "statefulset/${DEPLOYMENT_NAME}-mysql" --timeout="${WAIT_TIMEOUT}"
  runCmd "${KUBECTL_BIN}" -n "${NAMESPACE}" rollout status "${REDIS_WORKLOAD_KIND}/${DEPLOYMENT_NAME}-redis" --timeout="${WAIT_TIMEOUT}"
}

startPortForward() {
  if ! isTrue "${ENABLE_PORT_FORWARD}" || isTrue "${DRY_RUN}"; then
    return 0
  fi

  local port_log="${TMPDIR:-/tmp}/eruun-port-forward.log"
  nohup "${KUBECTL_BIN}" -n "${NAMESPACE}" port-forward "svc/${SERVICE_NAME}" "${LOCAL_PORT}:${REMOTE_PORT}" > "${port_log}" 2>&1 &
  PORT_FORWARD_PID=$!
  logInfo "Port-forward started with PID ${PORT_FORWARD_PID}; log: ${port_log}"
}

showAccessHints() {
  printf '\nEruun API access:\n'
  printf '  kubectl -n %s port-forward svc/%s %s:%s\n' "${NAMESPACE}" "${SERVICE_NAME}" "${LOCAL_PORT}" "${REMOTE_PORT}"
  printf '  curl --fail http://127.0.0.1:%s/api/v1/healthz\n' "${LOCAL_PORT}"
  printf '  curl --fail http://127.0.0.1:%s/api/v1/readyz\n\n' "${LOCAL_PORT}"
}

main() {
  printf 'Eruun all-in-one installer\n'
  preflightCheck
  confirmInstall

  if [ "${INSTALL_MODE}" = "helm" ]; then
    installHelm
  else
    installManifest
  fi

  removeLegacyClusterAdminBinding
  waitForReady
  startPortForward
  showAccessHints
  logSuccess "Eruun installation completed"
}

main "$@"
