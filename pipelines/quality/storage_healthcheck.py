# pipelines/quality/storage_healthcheck.py
# Collects per-table storage metrics (file counts, total size, avg size, snapshot count)
# and appends to lake.admin.storage_metrics for Grafana/ops.
#
# Usage:
#   ./scripts/spark_submit_local.sh pipelines/quality/storage_healthcheck.py --tables lake.gold.fact_trip_detailed, lake.silver.trip_enriched
from pyspark.sql import SparkSession, functions as F
import argparse

DEFAULT_TABLES = [
    "lake.gold.fact_trip_detailed",
    "lake.gold.fact_daily_zone_summary",
    "lake.gold.fact_hourly_demand",
    "lake.gold.fact_revenue_decomposition",
    "lake.silver.trip_enriched",
    "lake.bronze.trip_records",
]

def get_spark():
    return (
        SparkSession.builder
        .appName("storage_healthcheck")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type","hive")
        .config("spark.sql.catalog.lake.uri","thrift://hive-metastore:9083")
        .getOrCreate()
    )

def ensure_metrics_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.admin.storage_metrics (
            table_name STRING,
            run_ts TIMESTAMP,
            total_files BIGINT,
            total_size_bytes BIGINT,
            avg_file_size_bytes DOUBLE,
            small_files BIGINT,
            snapshot_count BIGINT
        ) USING iceberg
    """)

def collect_for_table(spark, table_name: str):
    # Iceberg metadata tables: <table>.files and <table>.snapshots
    files_df = spark.sql(f"SELECT file_size_in_bytes FROM {table_name}.files")
    snaps_df = spark.sql(f"SELECT snapshot_id FROM {table_name}.snapshots")

    if files_df.rdd.isEmpty():
        return spark.createDataFrame(
            [(table_name, F.current_timestamp(), 0, 0, 0.0, 0, snaps_df.count())],
            schema="table_name STRING, run_ts TIMESTAMP, total_files BIGINT, total_size_bytes BIGINT, avg_file_size_bytes DOUBLE, small_files BIGINT, snapshot_count BIGINT"
        )

    total_files = files_df.count()
    total_size = files_df.agg(F.sum("file_size_in_bytes")).first()[0] or 0
    avg_size = files_df.agg(F.avg("file_size_in_bytes")).first()[0] or 0.0
    # Flag "small files" as < 64 MiB
    small_files = files_df.filter(F.col("file_size_in_bytes") < 64 * 1024 * 1024).count()
    snap_count = snaps_df.count()

    return spark.createDataFrame(
        [(table_name, F.current_timestamp(), total_files, total_size, float(avg_size), small_files, snap_count)],
        schema="table_name STRING, run_ts TIMESTAMP, total_files BIGINT, total_size_bytes BIGINT, avg_file_size_bytes DOUBLE, small_files BIGINT, snapshot_count BIGINT"
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str, default=",".join(DEFAULT_TABLES))
    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    spark = get_spark()
    ensure_metrics_table(spark)

    out = None
    for t in tables:
        df = collect_for_table(spark, t)
        out = df if out is None else out.unionByName(df)

    out.writeTo("lake.admin.storage_metrics").append()
    spark.stop()

if __name__ == "__main__":
    main()
