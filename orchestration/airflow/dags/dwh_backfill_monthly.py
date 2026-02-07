# orchestration/airflow/dags/dwh_backfill_monthly.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

spark_submit = "/opt/spark/bin/spark-submit"
spark_conf = [
    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf", "spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "--conf", "spark.sql.catalog.lake.type=hive",
    "--conf", "spark.sql.catalog.lake.uri=thrift://hive-metastore:9083",
    "--conf", "spark.sql.warehouse.dir=s3a://nyc-tlc/warehouse",
    "--conf", "spark.hadoop.fs.s3a.endpoint="+os.getenv("S3_ENDPOINT","http://minio:9000"),
    "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf", "spark.hadoop.fs.s3a.access.key="+os.getenv("AWS_ACCESS_KEY_ID","minio"),
    "--conf", "spark.hadoop.fs.s3a.secret.key="+os.getenv("AWS_SECRET_ACCESS_KEY","minio123"),
    "--conf", "spark.openlineage.transport.type=http",
    "--conf", "spark.openlineage.transport.url="+os.getenv("OPENLINEAGE_URL","http://marquez:5000"),
    "--conf", "spark.openlineage.namespace="+os.getenv("OPENLINEAGE_NAMESPACE","nyc-taxi-dwh"),
]

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    dag_id="dwh_backfill_monthly",
    default_args=default_args,
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["production","backfill"]
) as dag:

    # Input variables (optional): month like '2019-01'; not strictly used now, but logged for audit
    month = Variable.get("backfill_month", default_var="2019-01")

    silver = BashOperator(
        task_id="silver_rebuild",
        bash_command=" ".join([
            spark_submit, *spark_conf, "/workspace/pipelines/transform/bronze_to_silver.py"
        ])
    )

    gold_full = BashOperator(
        task_id="gold_full_refresh",
        bash_command=" ".join([
            spark_submit, *spark_conf, "/workspace/pipelines/transform/silver_to_gold.py",
            "--full-refresh"
        ])
    )

    silver >> gold_full
