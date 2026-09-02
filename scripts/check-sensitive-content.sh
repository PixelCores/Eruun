#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
cd "${ROOT_DIR}"

failed=0

reportMatches() {
  local label="$1"
  shift
  printf 'Sensitive-content check failed: %s\n' "${label}" >&2
  "$@" >&2 || true
  failed=1
}

legacy_title='Kube''Min'
legacy_lower='kube''min'
legacy_upper='KUBE''MIN'
legacy_path='cmd/km''-rs'
legacy_pattern="${legacy_title}|${legacy_lower}|${legacy_upper}|${legacy_path}"

if rg -n -i --hidden --glob '!.git/**' --glob '!scripts/check-sensitive-content.sh' "${legacy_pattern}" . >/dev/null; then
  reportMatches "legacy product name or path remains" \
    rg -n -i --hidden --glob '!.git/**' --glob '!scripts/check-sensitive-content.sh' "${legacy_pattern}" .
fi

if find . -path './.git' -prune -o -print | grep -E -i "${legacy_pattern}" >/dev/null; then
  reportMatches "legacy product name remains in a path" \
    sh -c "find . -path './.git' -prune -o -print | grep -E -i '${legacy_pattern}'"
fi

private_domain='yu3''[.]co'
private_assets='3sdk''[.]'
private_owner='Silent''Echoe'
private_pattern="${private_domain}|${private_assets}|${private_owner}"
if rg -n -i --hidden --glob '!.git/**' --glob '!scripts/check-sensitive-content.sh' "${private_pattern}" . >/dev/null; then
  reportMatches "known private domain or owner reference remains" \
    rg -n -i --hidden --glob '!.git/**' --glob '!scripts/check-sensitive-content.sh' "${private_pattern}" .
fi

token_pattern='(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{30,}|sk-[A-Za-z0-9]{20,}|-----BEGIN ([A-Z0-9 ]+ )?PRIVATE KEY-----)'
if rg -n --hidden --glob '!.git/**' --glob '!scripts/check-sensitive-content.sh' "${token_pattern}" . >/dev/null; then
  reportMatches "token or private-key signature detected" \
    rg -n --hidden --glob '!.git/**' --glob '!scripts/check-sensitive-content.sh' "${token_pattern}" .
fi

credential_pattern='"[^"]*(password|passwd|clientSecret|accessKeySecret|api[-_]?key)[^"]*"[[:space:]]*:[[:space:]]*"[^"]{12,}"'
credential_matches=$(
  rg -n -i "${credential_pattern}" deploy config docs examples scripts \
    --glob '*.yaml' --glob '*.yml' --glob '*.json' --glob '*.md' --glob '*.sh' 2>/dev/null |
    rg -v '__REPLACE|replace-with|redacted|example[.]com|<[^>]+>|test[-_ ]|dummy|masked' || true
)
if [ -n "${credential_matches}" ]; then
  printf 'Sensitive-content check failed: credential-like literal detected\n%s\n' "${credential_matches}" >&2
  failed=1
fi

yaml_credential_pattern="(password|passwd|clientSecret|accessKeySecret|api[-_]?key)[[:space:]]*:[[:space:]]*['\"]?[A-Za-z0-9+/=_-]{12,}['\"]?[[:space:]]*(#.*)?$"
yaml_credential_matches=$(
  rg -n -i "${yaml_credential_pattern}" deploy config docs examples scripts \
    --glob '*.yaml' --glob '*.yml' 2>/dev/null |
    rg -v '__REPLACE|replace-with|redacted|example[.]com|<[^>]+>|test[-_ ]|dummy|masked' || true
)
if [ -n "${yaml_credential_matches}" ]; then
  printf 'Sensitive-content check failed: YAML credential-like literal detected\n%s\n' "${yaml_credential_matches}" >&2
  failed=1
fi

shell_default_pattern='(PASSWORD|PASSWD|TOKEN|SECRET)="[$][{][A-Z0-9_]+:-[^}]{4,}[}]"'
if rg -n "${shell_default_pattern}" deploy scripts --glob '*.sh' >/dev/null; then
  reportMatches "shell script contains a fixed credential default" \
    rg -n "${shell_default_pattern}" deploy scripts --glob '*.sh'
fi

if [ "${failed}" -ne 0 ]; then
  exit 1
fi

printf '%s\n' "Sensitive-content checks passed"
