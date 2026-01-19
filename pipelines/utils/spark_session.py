# pipelines/utils/spark_session.py
import os
from pyspark.sql import SparkSession

def get_spark(app_name: str = "nyc-taxi-bronze"):
    """
    Build a SparkSession configured for Iceberg + MinIO (S3A) using the Hive Metastore.
    """
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hive")
        .config("spark.sql.catalog.lake.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "s3a://nyc-tlc/warehouse")
        # S3A (MinIO)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        # Reasonable local dev defaults
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
    )
    return builder.getOrCreate()
