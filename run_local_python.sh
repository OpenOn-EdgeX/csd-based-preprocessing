#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="/root/miniconda3/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "error: local standard python not found: $PYTHON_BIN" >&2
  exit 1
fi

exec "$PYTHON_BIN" "$@"
