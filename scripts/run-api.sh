#!/usr/bin/env bash

set -euo pipefail

PYTHON_BIN="${PYTHON:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  else
    PYTHON_BIN="python"
  fi
fi

API_PORT="${IDC_API_PORT:-8001}"

if [[ -f "backend/.env" ]]; then
  # Use Python to safely parse the .env file (handles quoting, multi-line, etc.)
  eval "$("${PYTHON_BIN}" -c "
import sys, os
for line in open('backend/.env'):
    line = line.strip()
    if not line or line.startswith('#') or '=' not in line:
        continue
    key, _, value = line.partition('=')
    key = key.strip()
    value = value.strip()
    # Strip surrounding quotes
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('\"', \"'\"):
        value = value[1:-1]
    if not value:
        continue
    # Only export safe variable names
    if key.isidentifier():
        print(f'export {key}={chr(39)}{value.replace(chr(39), chr(39) + chr(92) + chr(39) + chr(39))}{chr(39)}')
")"
fi

if [[ "${1:-}" == "dev" ]]; then
  exec env PYTHONPATH=. "${PYTHON_BIN}" -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port "${API_PORT}"
fi

exec env PYTHONPATH=. "${PYTHON_BIN}" -m uvicorn backend.app.main:app --host 127.0.0.1 --port "${API_PORT}"
