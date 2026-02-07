#!/usr/bin/env bash
set -euo pipefail

OLDER="${OLDER:-30}"
RETAIN="${RETAIN:-10}"
TARGET_MB="${TARGET_MB:-256}"
MIN_FILES="${MIN_FILES:-5}"

echo "Running Iceberg maintenance now: older=${OLDER}d retain=${RETAIN} target=${TARGET_MB}MB min_files=${MIN_FILES}"
./scripts/spark_submit_local.sh pipelines/transform/maintenance_iceberg.py \
  --older-than-days "${OLDER}" \
  --retain-last "${RETAIN}" \
  --target-file-size-mb "${TARGET_MB}" \
  --min-input-files "${MIN_FILES}"

echo "Collecting storage metrics..."
./scripts/spark_submit_local.sh pipelines/quality/storage_healthcheck.py
