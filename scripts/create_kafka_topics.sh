#!/usr/bin/env bash
set -euo pipefail

BROKER="${BROKER:-kafka:9092}"
PARTITIONS="${PARTITIONS:-3}"
RETENTION_MS="${RETENTION_MS:-172800000}"  # 2 days

topics=(yellow_trips green_trips hvfhs_trips)

for t in "${topics[@]}"; do
  echo "Creating topic: ${t}"
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BROKER}" \
    --create --if-not-exists \
    --topic "${t}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1 \
    --config retention.ms="${RETENTION_MS}"
done

echo "Topics created. Listing:"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BROKER}" --list
