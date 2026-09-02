#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

export INSTALL_MODE="${INSTALL_MODE:-manifest}"
export MANIFEST="${MANIFEST:-${ROOT_DIR}/deploy/eruun-stack.yaml}"
exec "${ROOT_DIR}/deploy/all_in_one_install_quickstart.sh" "$@"
