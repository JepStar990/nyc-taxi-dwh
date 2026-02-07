#!/usr/bin/env bash
set -euo pipefail
VAULT_ADDR=${VAULT_ADDR:-http://localhost:8200}
VAULT_TOKEN=${VAULT_TOKEN:-myroot}

echo "Writing demo secrets to Vault (KV v2)..."
curl -sS --header "X-Vault-Token: ${VAULT_TOKEN}" \
  --request POST \
  --data '{"data":{"webhook":"https://hooks.slack.com/services/REPLACE/ME/PLEASE"}}' \
  ${VAULT_ADDR}/v1/secret/data/airflow/slack

echo "Done. Path: secret/data/airflow/slack key: webhook"
