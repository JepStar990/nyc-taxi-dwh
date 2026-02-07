from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

spark_submit = "/opt/spark/bin/spark-submit"
spark_conf = [
    "--conf","spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf","spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "--conf","spark.sql.catalog.lake.type=hive",
    "--conf","spark.sql.catalog.lake.uri=thrift://hive-metastore:9083",
]

default_args = {"owner":"data-eng","retries":1,"retry_delay":timedelta(minutes=10)}

with DAG(
    dag_id="data_sharing_exports",
    default_args=default_args,
    start_date=datetime(2025,1,1),
    schedule_interval="0 6 * * *",  # daily 06:00 UTC
    catchup=False,
    tags=["production","data-sharing"]
) as dag:

    export_gold = BashOperator(
        task_id="export_gold_snapshots",
        bash_command=" ".join([
            spark_submit,*spark_conf,
            "/workspace/pipelines/sharing/export_gold_shares.py",
            "--tables","lake.gold.fact_daily_zone_summary,lake.gold.fact_revenue_decomposition"
        ])
    )

    # Generate presigned URLs for the latest snapshot folders
    presign = BashOperator(
        task_id="presign_latest",
        bash_command=r"""
          set -e
          docker compose exec -T mc mc alias set local http://minio:9000 minio minio123
          for t in fact_daily_zone_summary fact_revenue_decomposition; do
            latest=$(docker compose exec -T mc mc ls local/nyc-tlc/shares/$t/ | awk '{print $NF}' | sort | tail -n1 | tr -d '\r')
            echo "Latest for $t: $latest"
            docker compose exec -T mc mc ls local/nyc-tlc/shares/$t/"$latest"
            # presign each file in snapshot
            docker compose exec -T mc sh -lc "for f in \$(mc ls --json local/nyc-tlc/shares/$t/$latest | jq -r '.key'); do mc share link --expire 24h local/nyc-tlc/shares/$t/$latest/\$f; done"
          done
        """
    )

    export_gold >> presign
