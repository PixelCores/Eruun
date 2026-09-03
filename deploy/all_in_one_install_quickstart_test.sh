#!/usr/bin/env bash

set -Eeuo pipefail

if [ "$#" -ge 1 ] && [ "$1" = "rand" ]; then
  printf '%s\n' '0123456789abcdef0123456789abcdef0123456789abcdef'
  exit 0
fi

if [ "${ERUUN_QUICKSTART_FAKE_COMMAND:-false}" = "true" ]; then
  printf '%s\n' "$*" >> "${FAKE_COMMAND_LOG}"
  if [ -n "${FAKE_FAIL_COMMAND:-}" ] && [ "$*" = "${FAKE_FAIL_COMMAND}" ]; then
    exit 1
  fi
  case "$*" in
    *"create secret generic"*)
      printf '%s\n' 'apiVersion: v1' 'kind: Secret' 'metadata:' '  name: fake'
      ;;
  esac
  exit 0
fi

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEST_SCRIPT="${TEST_DIR}/all_in_one_install_quickstart_test.sh"
INSTALLER_SOURCE="${TEST_DIR}/all_in_one_install_quickstart.sh"
MANIFEST_SOURCE="${TEST_DIR}/eruun-stack.yaml"
CHART_SOURCE="${TEST_DIR}/helm/eruun"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/eruun-quickstart-test.XXXXXX")
trap 'rm -rf "${TEST_ROOT}"' EXIT
export AUTH_CONFIG_FILE="${TEST_ROOT}/accounts.json"
printf '%s\n' '{"bootstrapAdmin":{"email":"admin@example.com","password":"test-only-bootstrap-password"}}' > "${AUTH_CONFIG_FILE}"
chmod 600 "${AUTH_CONFIG_FILE}"

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assertContains() {
  grep -Fq -- "$2" "$1" || fail "$1 does not contain: $2"
}

assertNotContains() {
  if grep -Fq -- "$2" "$1"; then
    fail "$1 unexpectedly contains: $2"
  fi
}

newCase() {
  local name="$1"
  local case_dir="${TEST_ROOT}/${name}"
  mkdir -p "${case_dir}/tmp"
  cp "${INSTALLER_SOURCE}" "${case_dir}/installer.sh"
  cp "${MANIFEST_SOURCE}" "${case_dir}/eruun-stack.yaml"
  chmod +x "${case_dir}/installer.sh"
  printf '%s\n' "${case_dir}"
}

assertTempFilesCleaned() {
  local case_dir="$1"
  if find "${case_dir}/tmp" -type f -print -quit | grep -q .; then
    fail "sensitive temporary files were not cleaned in ${case_dir}/tmp"
  fi
}

runManifestGeneratedSecretsContract() {
  local case_dir
  case_dir=$(newCase manifest-generated-secrets)
  local command_log="${case_dir}/commands.log"
  local output="${case_dir}/output.log"

  env \
    ERUUN_QUICKSTART_FAKE_COMMAND=true \
    FAKE_COMMAND_LOG="${command_log}" \
    INSTALL_MODE=manifest \
    MANIFEST="${case_dir}/eruun-stack.yaml" \
    KUBECTL_BIN="${TEST_SCRIPT}" \
    OPENSSL_BIN="${TEST_SCRIPT}" \
    TMPDIR="${case_dir}/tmp" \
    SKIP_CONFIRM=true \
    WAIT_READY=false \
    "${case_dir}/installer.sh" > "${output}" 2>&1

  assertContains "${command_log}" "create secret generic eruun-mysql-secret"
  assertContains "${command_log}" "create secret generic eruun-secret"
  assertContains "${command_log}" "create secret generic eruun-account-config"
  assertNotContains "${output}" "test-only-bootstrap-password"
  assertContains "${command_log}" "apply --dry-run=server -f ${case_dir}/eruun-stack.yaml"
  assertContains "${command_log}" "apply -f ${case_dir}/eruun-stack.yaml"
  assertContains "${command_log}" "delete clusterrolebinding eruun-platform-cluster-admin --ignore-not-found=true"
  assertContains "${output}" "curl --fail http://127.0.0.1:8000/api/v1/healthz"
  assertNotContains "${output}" "0123456789abcdef0123456789abcdef0123456789abcdef"
  assertTempFilesCleaned "${case_dir}"
}

runHelmSensitiveValuesContract() {
  local case_dir
  case_dir=$(newCase helm-sensitive-values)
  local command_log="${case_dir}/commands.log"
  local output="${case_dir}/output.log"

  env \
    ERUUN_QUICKSTART_FAKE_COMMAND=true \
    FAKE_COMMAND_LOG="${command_log}" \
    INSTALL_MODE=helm \
    CHART_PATH="${CHART_SOURCE}" \
    KUBECTL_BIN="${TEST_SCRIPT}" \
    HELM_BIN="${TEST_SCRIPT}" \
    OPENSSL_BIN="${TEST_SCRIPT}" \
    TMPDIR="${case_dir}/tmp" \
    SKIP_CONFIRM=true \
    WAIT_READY=false \
    "${case_dir}/installer.sh" > "${output}" 2>&1

  assertContains "${command_log}" "upgrade --install eruun ${CHART_SOURCE}"
  assertContains "${command_log}" "--values ${case_dir}/tmp/eruun-helm-sensitive-values."
  assertNotContains "${command_log}" "delete clusterrolebinding"
  assertNotContains "${output}" "0123456789abcdef0123456789abcdef0123456789abcdef"
  assertTempFilesCleaned "${case_dir}"
}

runLegacyBindingCleanupScopeContract() {
  local scenario
  for scenario in helm-other-namespace helm-other-fullname custom-manifest custom-namespace apply-failure; do
    local case_dir
    case_dir=$(newCase "${scenario}")
    local command_log="${case_dir}/commands.log"
    local output="${case_dir}/output.log"
    local mode=manifest
    local namespace=eruun-system
    local fullname=eruun
    local manifest="${case_dir}/eruun-stack.yaml"
    local fail_command=""
    case "${scenario}" in
      helm-other-namespace) mode=helm; namespace=another-eruun ;;
      helm-other-fullname) mode=helm; fullname=another-eruun ;;
      custom-manifest)
        manifest="${case_dir}/custom-stack.yaml"
        cp "${MANIFEST_SOURCE}" "${manifest}"
        ;;
      custom-namespace) namespace=another-eruun ;;
      apply-failure) fail_command="apply -f ${manifest}" ;;
    esac

    local status=0
    env \
      ERUUN_QUICKSTART_FAKE_COMMAND=true \
      FAKE_COMMAND_LOG="${command_log}" \
      FAKE_FAIL_COMMAND="${fail_command}" \
      INSTALL_MODE="${mode}" \
      NAMESPACE="${namespace}" \
      FULLNAME_OVERRIDE="${fullname}" \
      MANIFEST="${manifest}" \
      CHART_PATH="${CHART_SOURCE}" \
      KUBECTL_BIN="${TEST_SCRIPT}" \
      HELM_BIN="${TEST_SCRIPT}" \
      OPENSSL_BIN="${TEST_SCRIPT}" \
      MYSQL_ROOT_PASSWORD=test-root \
      REDIS_PASSWORD=test-redis \
      MYSQL_PASSWORD=test-mysql \
      TMPDIR="${case_dir}/tmp" \
      SKIP_CONFIRM=true \
      WAIT_READY=false \
      "${case_dir}/installer.sh" > "${output}" 2>&1 || status=$?

    if [ "${scenario}" = apply-failure ]; then
      [ "${status}" -ne 0 ] || fail "failed manifest apply must fail the installation"
    else
      [ "${status}" -eq 0 ] || fail "${scenario} installation unexpectedly failed"
    fi
    assertNotContains "${command_log}" "delete clusterrolebinding"
    assertTempFilesCleaned "${case_dir}"
  done
}

runPlaceholderRejectionContract() {
  local case_dir
  case_dir=$(newCase placeholder-rejection)
  local command_log="${case_dir}/commands.log"
  local output="${case_dir}/output.log"
  : > "${command_log}"

  if env \
    ERUUN_QUICKSTART_FAKE_COMMAND=true \
    FAKE_COMMAND_LOG="${command_log}" \
    INSTALL_MODE=manifest \
    MANIFEST="${case_dir}/eruun-stack.yaml" \
    KUBECTL_BIN="${TEST_SCRIPT}" \
    OPENSSL_BIN="${TEST_SCRIPT}" \
    MYSQL_ROOT_PASSWORD=__REPLACE_WITH_STRONG_PASSWORD__ \
    REDIS_PASSWORD=test-only \
    MYSQL_PASSWORD=test-only \
    TMPDIR="${case_dir}/tmp" \
    SKIP_CONFIRM=true \
    "${case_dir}/installer.sh" > "${output}" 2>&1; then
    fail "placeholder credentials must be rejected"
  fi

  assertContains "${output}" "contains unresolved placeholder text"
  assertNotContains "${command_log}" "create secret generic"
  assertTempFilesCleaned "${case_dir}"
}

runDryRunContract() {
  local case_dir
  case_dir=$(newCase dry-run)
  local command_log="${case_dir}/commands.log"
  local output="${case_dir}/output.log"

  env \
    ERUUN_QUICKSTART_FAKE_COMMAND=true \
    FAKE_COMMAND_LOG="${command_log}" \
    INSTALL_MODE=manifest \
    MANIFEST="${case_dir}/eruun-stack.yaml" \
    KUBECTL_BIN="${TEST_SCRIPT}" \
    OPENSSL_BIN="${TEST_SCRIPT}" \
    MYSQL_ROOT_PASSWORD=test-root \
    REDIS_PASSWORD=test-redis \
    MYSQL_PASSWORD=test-mysql \
    DRY_RUN=true \
    TMPDIR="${case_dir}/tmp" \
    SKIP_CONFIRM=true \
    "${case_dir}/installer.sh" > "${output}" 2>&1

  assertContains "${command_log}" "apply --dry-run=client -f"
  assertContains "${command_log}" "delete clusterrolebinding eruun-platform-cluster-admin --ignore-not-found=true --dry-run=client"
  assertNotContains "${command_log}" "apply --dry-run=server"
  assertTempFilesCleaned "${case_dir}"
}

runManifestGeneratedSecretsContract
runHelmSensitiveValuesContract
runLegacyBindingCleanupScopeContract
runPlaceholderRejectionContract
runDryRunContract

printf '%s\n' "Eruun quickstart tests passed"
