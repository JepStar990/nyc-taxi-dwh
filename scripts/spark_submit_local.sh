#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <python_or_jar_path> [args...]"
  exit 1
fi

APP_PATH="$1"; shift || true

# Make your project (mounted at /workspace) importable:
#  - PYTHONPATH for the driver
#  - spark.executorEnv.PYTHONPATH for executors
docker compose exec -T \
  -e PYTHONPATH=/workspace \
  spark /opt/spark/bin/spark-submit \
  --conf spark.executorEnv.PYTHONPATH=/workspace \
  --conf spark.driver.extraJavaOptions="-Dconfig.file=/opt/spark/conf/iceberg-catalog.conf" \
  --conf spark.executor.extraJavaOptions="-Dconfig.file=/opt/spark/conf/iceberg-catalog.conf" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.697 \
  "/workspace/${APP_PATH}" "$@"
