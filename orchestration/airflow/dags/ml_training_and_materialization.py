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
    "--conf","spark.sql.warehouse.dir=s3a://nyc-tlc/warehouse",
    "--conf","spark.hadoop.fs.s3a.endpoint="+os.getenv("S3_ENDPOINT","http://minio:9000"),
    "--conf","spark.hadoop.fs.s3a.path.style.access=true",
    "--conf","spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf","spark.hadoop.fs.s3a.access.key="+os.getenv("AWS_ACCESS_KEY_ID","minio"),
    "--conf","spark.hadoop.fs.s3a.secret.key="+os.getenv("AWS_SECRET_ACCESS_KEY","minio123"),
]

default_args = {"owner":"mlops","retries":1,"retry_delay":timedelta(minutes=10)}

with DAG(
    dag_id="ml_training_and_materialization",
    default_args=default_args,
    start_date=datetime(2025,1,1),
    schedule_interval="0 2 * * *",  # daily 02:00 UTC
    catchup=False,
    tags=["production","ml"]
) as dag:

    train = BashOperator(
        task_id="train_forecast",
        bash_command=" ".join([spark_submit,*spark_conf,"/workspace/ml/pipelines/train_forecast_spark.py"])
    )

    infer = BashOperator(
        task_id="batch_inference_next_hour",
        bash_command=" ".join([spark_submit,*spark_conf,"/workspace/ml/pipelines/batch_inference_forecast_spark.py"])
    )

    # Feast materialization (offline->online Redis) if you’re using Feast features in serving
    feast_mat = BashOperator(
        task_id="feast_materialize_7d",
        bash_command="""
          cd /workspace/ml/features/feast_repo && \
          feast materialize-incremental $(date -u +%Y-%m-%d)
        """
    )

    train >> infer >> feast_mat
