#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

"${SCRIPT_DIR}/check-sensitive-content.sh"
"${SCRIPT_DIR}/check-licenses.sh"

printf '%s\n' "Open-source readiness checks passed"
