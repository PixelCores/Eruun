#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

usage() {
  cat <<'MSG'
Usage: scripts/count-code-lines.sh [--include-config]

Counts tracked Eruun source lines by category.

Default scope:
  *.go, *.sh, *.sql, Makefile, *.mk, Dockerfile

Options:
  --include-config  Also count tracked config/module files:
                    *.yaml, *.yml, *.json, *.toml, go.mod, go.sum,
  -h, --help        Show this help text.
MSG
}

include_config=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --include-config)
      include_config=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v git >/dev/null 2>&1; then
  echo "git not found. This script counts tracked files via git ls-files." >&2
  exit 1
fi

if ! git -C "${ROOT_DIR}" rev-parse --show-toplevel >/dev/null 2>&1; then
  echo "Not inside a git repository: ${ROOT_DIR}" >&2
  exit 1
fi

source_patterns=(
  "*.go"
  "*.sh"
  "*.sql"
  "*.mk"
  "Makefile"
  "*/Makefile"
  "Dockerfile"
  "*/Dockerfile"
  "*.Dockerfile"
)

config_patterns=(
  "*.yaml"
  "*.yml"
  "*.json"
  "*.toml"
  "go.mod"
  "*/go.mod"
  "go.sum"
  "*/go.sum"
)

patterns=("${source_patterns[@]}")
scope="tracked source files"
if [[ "${include_config}" -eq 1 ]]; then
  patterns+=("${config_patterns[@]}")
  scope="tracked source and config/module files"
fi

category_for_file() {
  local file="$1"

  case "${file}" in
    *.go)
      echo "Go"
      ;;
    *.sh)
      echo "Shell"
      ;;
    *.sql)
      echo "SQL"
      ;;
    *.mk|Makefile|*/Makefile)
      echo "Make"
      ;;
    Dockerfile|*/Dockerfile|*.Dockerfile)
      echo "Docker"
      ;;
    *.yaml|*.yml)
      echo "YAML"
      ;;
    *.json)
      echo "JSON"
      ;;
    *.toml)
      echo "TOML"
      ;;
    go.mod|*/go.mod|go.sum|*/go.sum)
      echo "Go module"
      ;;
    *)
      echo "Other"
      ;;
  esac
}

tmp_file=$(mktemp)
cleanup() {
  rm -f "${tmp_file}"
}
trap cleanup EXIT

cd "${ROOT_DIR}"

while IFS= read -r -d '' file; do
  [[ -f "${file}" ]] || continue

  metrics=$(awk '
    NF {
      nonblank++
    }
    END {
      printf "%d %d", NR, nonblank + 0
    }
  ' "${file}")
  total_lines=${metrics%% *}
  nonblank_lines=${metrics##* }
  category=$(category_for_file "${file}")

  printf "%s\t%s\t%s\t%s\n" "${category}" "${total_lines}" "${nonblank_lines}" "${file}" >>"${tmp_file}"
done < <(git ls-files -z -- "${patterns[@]}")

if [[ ! -s "${tmp_file}" ]]; then
  echo "No tracked files matched the selected scope."
  exit 0
fi

echo "Eruun code line count"
echo "Root: ${ROOT_DIR}"
echo "Scope: ${scope}"
echo

printf "%-12s %8s %12s %12s\n" "Category" "Files" "Lines" "Nonblank"
printf "%-12s %8s %12s %12s\n" "--------" "-----" "-----" "--------"

awk -F '\t' '
  {
    files[$1]++
    lines[$1] += $2
    nonblank[$1] += $3
  }
  END {
    for (category in files) {
      printf "%s\t%d\t%d\t%d\n", category, files[category], lines[category], nonblank[category]
    }
  }
' "${tmp_file}" | sort | awk -F '\t' '{
  printf "%-12s %8d %12d %12d\n", $1, $2, $3, $4
}'

total_files=$(awk 'END { print NR }' "${tmp_file}")
total_lines=$(awk -F '\t' '{ sum += $2 } END { print sum + 0 }' "${tmp_file}")
total_nonblank=$(awk -F '\t' '{ sum += $3 } END { print sum + 0 }' "${tmp_file}")

printf "%-12s %8s %12s %12s\n" "--------" "-----" "-----" "--------"
printf "%-12s %8d %12d %12d\n" "Total" "${total_files}" "${total_lines}" "${total_nonblank}"
