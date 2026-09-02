#!/usr/bin/env bash

set -euo pipefail
umask 077

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
OVERRIDES="${ROOT_DIR}/third_party/license-overrides.csv"
INVENTORY="${ROOT_DIR}/THIRD_PARTY_LICENSES.csv"
GO_LICENSES_VERSION=v2.0.1
MODE="${1:-check}"

case "${MODE}" in
  check|--update) ;;
  *)
    printf 'Usage: %s [check|--update]\n' "$0" >&2
    exit 2
    ;;
esac

command -v go >/dev/null 2>&1 || {
  printf '%s\n' "go is required" >&2
  exit 127
}

test -f "${OVERRIDES}" || {
  printf 'missing license override registry: %s\n' "${OVERRIDES}" >&2
  exit 1
}

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/eruun-license-check.XXXXXX")
cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

GOBIN="${WORK_DIR}/bin" go install "github.com/google/go-licenses/v2@${GO_LICENSES_VERSION}"
GO_LICENSES="${WORK_DIR}/bin/go-licenses"
RAW_REPORT="${WORK_DIR}/raw.csv"
GENERATED="${WORK_DIR}/generated.csv"
UNKNOWN_PACKAGES="${WORK_DIR}/unknown-packages.txt"
OVERRIDE_PACKAGES="${WORK_DIR}/override-packages.txt"
REPORT_PACKAGES="${WORK_DIR}/report-packages.txt"
BODY="${WORK_DIR}/body.csv"

cd "${ROOT_DIR}"
"${GO_LICENSES}" report --include_tests ./... > "${RAW_REPORT}"

awk -F, '$2 == "Unknown" && $3 == "Unknown" { print $1 }' "${RAW_REPORT}" | LC_ALL=C sort -u > "${UNKNOWN_PACKAGES}"
awk -F, 'NR > 1 { print $1 }' "${OVERRIDES}" | LC_ALL=C sort -u > "${OVERRIDE_PACKAGES}"
awk -F, '{ print $1 }' "${RAW_REPORT}" | LC_ALL=C sort -u > "${REPORT_PACKAGES}"

unapproved_unknown=$(comm -23 "${UNKNOWN_PACKAGES}" "${OVERRIDE_PACKAGES}" || true)
if [ -n "${unapproved_unknown}" ]; then
  printf 'unapproved unknown license packages:\n%s\n' "${unapproved_unknown}" >&2
  exit 1
fi

stale_overrides=$(comm -23 "${OVERRIDE_PACKAGES}" "${REPORT_PACKAGES}" || true)
if [ -n "${stale_overrides}" ]; then
  printf 'stale license overrides not present in the dependency report:\n%s\n' "${stale_overrides}" >&2
  exit 1
fi

ignore_args=()
while IFS=, read -r package _; do
  [ "${package}" = "package" ] && continue
  ignore_args+=(--ignore "${package}")
done < "${OVERRIDES}"

"${GO_LICENSES}" check \
  --include_tests \
  --disallowed_types=forbidden,restricted,unknown \
  "${ignore_args[@]}" \
  ./...

awk -F, '
  NR == FNR {
    if (FNR > 1) {
      overridden[$1] = 1
    }
    next
  }
  !($1 in overridden)
' "${OVERRIDES}" "${RAW_REPORT}" > "${BODY}"

awk -F, 'NR > 1 { print $1 "," $2 "," $3 }' "${OVERRIDES}" >> "${BODY}"
{
  printf '%s\n' "package,license_url,license"
  LC_ALL=C sort -u "${BODY}"
} > "${GENERATED}"

if rg -n ',Unknown(,Unknown)?$' "${GENERATED}" >/dev/null; then
  printf '%s\n' "generated license inventory still contains unknown classifications" >&2
  rg -n ',Unknown(,Unknown)?$' "${GENERATED}" >&2
  exit 1
fi

if [ "${MODE}" = "--update" ]; then
  cp "${GENERATED}" "${INVENTORY}"
  chmod 0644 "${INVENTORY}"
  printf 'updated %s with go-licenses/v2@%s\n' "${INVENTORY}" "${GO_LICENSES_VERSION}"
  exit 0
fi

test -f "${INVENTORY}" || {
  printf 'missing generated inventory: %s; run %s --update\n' "${INVENTORY}" "$0" >&2
  exit 1
}

if ! diff -u "${INVENTORY}" "${GENERATED}"; then
  printf 'license inventory is stale; run %s --update\n' "$0" >&2
  exit 1
fi

printf 'License checks passed with go-licenses/v2@%s\n' "${GO_LICENSES_VERSION}"
