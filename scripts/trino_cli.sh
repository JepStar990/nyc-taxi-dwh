#!/usr/bin/env bash
set -euo pipefail

SQL_FILE="${1:-}"
if [ -z "${SQL_FILE}" ]; then
  echo "Usage: $0 <sql-file>"
  exit 1
fi
if [ ! -f "${SQL_FILE}" ]; then
  echo "SQL file not found: ${SQL_FILE}"
  exit 1
fi

if command -v trino >/dev/null 2>&1; then
  trino --server http://localhost:8080 --execute "$(cat "${SQL_FILE}")"
else
  # Minimal REST submit (no auth)
  PAYLOAD=$(jq -Rn --slurpfile q "${SQL_FILE}" '{query: ($q|join("")) }')
  curl -sS -X POST -H "X-Trino-User: trino" -H "Content-Type: application/json" \
       --data "${PAYLOAD}" http://localhost:8080/v1/statement
fi
