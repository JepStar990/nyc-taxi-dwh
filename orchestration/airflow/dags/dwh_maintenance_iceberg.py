# orchestration/airflow/dags/dwh_maintenance_iceberg.py
from datetime import datetime, timedelta
import os
from airflow import DAG
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
    # OpenLineage for ops observability
    "--conf", "spark.openlineage.transport.type=http",
    "--conf", "spark.openlineage.transport.url="+os.getenv("OPENLINEAGE_URL","http://marquez:5000"),
    "--conf", "spark.openlineage.namespace="+os.getenv("OPENLINEAGE_NAMESPACE","nyc-taxi-dwh"),
]

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="dwh_maintenance_iceberg",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 3 * * SUN",  # Sundays at 03:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["production","maintenance","cost"]
) as dag:

    wait_services = BashOperator(
        task_id="wait_services",
        bash_command="""
        for i in {1..60}; do nc -z hive-metastore 9083 && break; sleep 2; done
        for i in {1..60}; do nc -z minio 9000 && break; sleep 2; done
        echo "Dependencies up"
        """
    )

    # Compact + expire snapshots with safe defaults
    maintenance = BashOperator(
        task_id="iceberg_compaction_and_expiry",
        bash_command=" ".join([
            spark_submit, *spark_conf,
            "/workspace/pipelines/transform/maintenance_iceberg.py",
            "--older-than-days", os.getenv("ICEBERG_EXPIRE_DAYS","30"),
            "--retain-last", os.getenv("ICEBERG_RETAIN_LAST","10"),
            "--target-file-size-mb", os.getenv("ICEBERG_TARGET_FILE_MB","256"),
            "--min-input-files", os.getenv("ICEBERG_MIN_INPUT_FILES","5"),
        ])
    )

    # Persist storage metrics for dashboards
    metrics = BashOperator(
        task_id="storage_health_metrics",
        bash_command=" ".join([
            spark_submit, *spark_conf,
            "/workspace/pipelines/quality/storage_healthcheck.py"
        ])
    )

    wait_services >> maintenance >> metrics
