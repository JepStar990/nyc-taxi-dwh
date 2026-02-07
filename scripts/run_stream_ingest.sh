#!/usr/bin/env bash
set -euo pipefail

MODE="${1:---continuous}"  # or --available-now

echo "Starting streaming job in mode: ${MODE}"

docker compose exec -d spark /opt/spark/bin/spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hive \
  --conf spark.sql.catalog.lake.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=s3a://nyc-tlc/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT:-http://minio:9000} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-minio} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-minio123} \
  /workspace/pipelines/ingestion/stream_to_bronze_spark.py ${MODE}
