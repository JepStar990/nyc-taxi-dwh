#!/usr/bin/env bash
set -euo pipefail

TABLE="${1:-lake.gold.fact_trip_detailed}"
SNAPSHOT="${2:-}"

if [ -z "$SNAPSHOT" ]; then
  echo "Usage: $0 <table> <snapshot_id>"
  exit 1
fi

echo "Reading snapshot $SNAPSHOT from $TABLE and overwriting current content..."
docker compose exec -T spark /opt/spark/bin/spark-sql -e "
  CREATE OR REPLACE TEMP VIEW _snap AS
  SELECT * FROM ${TABLE} FOR VERSION AS OF ${SNAPSHOT};
  INSERT OVERWRITE TABLE ${TABLE} SELECT * FROM _snap;
"
echo "Rollback complete."
