import os
from pyspark.sql import SparkSession, functions as F

def build_spark(app_name="seed_raw_from_samples"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hive")
        .config("spark.sql.catalog.lake.uri", "thrift://hive-metastore:9083")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
        # Warehousing & performance
        .config("spark.sql.warehouse.dir", "s3a://nyc-tlc/warehouse")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    return spark

def write_raw_table(spark, name, src_path, time_cols):
    print(f"==> Seeding lake.raw.{name} from {src_path}")
    df = spark.read.parquet(src_path)

    # Normalize timestamp columns to TIMESTAMP type if present
    for c in time_cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))

    # Add ingest metadata (immutable RAW)
    df = df.withColumn("ingest_time", F.current_timestamp()) \
           .withColumn("source_path", F.lit(src_path))

    (df
     .writeTo(f"lake.raw.{name}")
     .using("iceberg")
     .tableProperty("format-version","2")
     .tableProperty("write.format.default","parquet")
     .createOrReplace())

def main():
    spark = build_spark()

    # Source sample files (uploaded by scripts/bootstrap_docker.sh)
    base = "s3a://nyc-tlc/raw"

    datasets = [
        ("yellow", f"{base}/yellow/2019-01.parquet",
         ["tpep_pickup_datetime","tpep_dropoff_datetime"]),
        ("green", f"{base}/green/2019-01.parquet",
         ["lpep_pickup_datetime","lpep_dropoff_datetime"]),
        ("hvfhs", f"{base}/hvfhs/2019-03.parquet",
         ["request_datetime","pickup_datetime","dropoff_datetime"]),
        ("fhv", f"{base}/fhv/2018-10.parquet",
         ["pickup_datetime","dropOff_datetime"]),
    ]

    for name, path, tcols in datasets:
        write_raw_table(spark, name, path, tcols)

    print("==> RAW tables created successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
