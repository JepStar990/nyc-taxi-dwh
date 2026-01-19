#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Building images (spark, trino, superset)..."
docker compose -f "${ROOT_DIR}/docker-compose.yml" build

echo "==> Starting services..."
docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d

echo "==> Waiting for MinIO (9000), Hive Metastore (9083), Trino (8080)..."
# Simple wait loops
for i in {1..60}; do
  if nc -z localhost 9000; then break; fi; sleep 2
done
for i in {1..60}; do
  if nc -z localhost 9083; then break; fi; sleep 2
done
for i in {1..60}; do
  if nc -z localhost 8080; then break; fi; sleep 2
done

echo "==> Configuring MinIO client alias..."
docker compose exec -T mc mc alias set local http://minio:9000 minio minio123

echo "==> Creating bucket s3://nyc-tlc if missing..."
if ! docker compose exec -T mc mc ls local/nyc-tlc >/dev/null 2>&1; then
  docker compose exec -T mc mc mb local/nyc-tlc
fi

echo "==> Syncing sample data to MinIO (raw/ and external/)..."
docker compose exec -T mc mc mirror --overwrite /data/samples local/nyc-tlc/raw
docker compose exec -T mc mc mirror --overwrite /data/external local/nyc-tlc/external

echo "==> Creating Iceberg namespaces using Spark SQL..."
docker compose exec -T spark /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hive \
  --conf spark.sql.catalog.lake.uri=thrift://hive-metastore:9083 \
  -f /workspace/sql/ddl/icebergs.sql

echo "==> Seeding RAW tables from sample parquet files..."
docker compose exec -T spark /opt/spark/bin/spark-submit \
  --conf spark.driver.extraJavaOptions="-Dconfig.file=/opt/spark/conf/iceberg-catalog.conf" \
  --conf spark.executor.extraJavaOptions="-Dconfig.file=/opt/spark/conf/iceberg-catalog.conf" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.697 \
  /workspace/pipelines/ingestion/seed_raw_from_samples.py

echo "==> Done. Open:"
echo "    - MinIO Console:    http://localhost:9001  (minio/minio123)"
echo "    - Trino Web (UI):   http://localhost:8080"
echo "    - Superset:         http://localhost:8088"

