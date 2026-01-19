#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T mc mc alias set local http://minio:9000 minio minio123
docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T mc mc mb --ignore-existing local/nyc-tlc
docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T mc mc mirror --overwrite /data/samples local/nyc-tlc/raw
docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T mc mc mirror --overwrite /data/external local/nyc-tlc/external
