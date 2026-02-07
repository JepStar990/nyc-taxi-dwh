#!/usr/bin/env bash
set -euo pipefail
TABLE="${1:?table name (e.g., fact_daily_zone_summary)}"
docker compose exec -T mc mc alias set local http://minio:9000 minio minio123
LATEST=$(docker compose exec -T mc mc ls local/nyc-tlc/shares/$TABLE/ | awk '{print $NF}' | sort | tail -n1 | tr -d '\r')
docker compose exec -T mc sh -lc "for f in \$(mc ls --json local/nyc-tlc/shares/$TABLE/$LATEST | jq -r '.key'); do mc share link --expire 72h local/nyc-tlc/shares/$TABLE/$LATEST/\$f; done"
