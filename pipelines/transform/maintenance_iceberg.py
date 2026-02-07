# pipelines/transform/maintenance_iceberg.py
# Runs Iceberg maintenance procedures for selected tables:
#  - rewrite_data_files (compaction + applies table write order)
#  - rewrite_manifests   (reduce manifest overhead)
#  - expire_snapshots    (retention policy)
#
# Usage:
#   ./scripts/spark_submit_local.sh pipelines/transform/maintenance_iceberg.py --older-than-days 30 --retain-last 10
#   (Airflow DAG provided in orchestration/airflow/dags/dwh_maintenance_iceberg.py)
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

CATALOG = "lake"

DEFAULT_TABLES = [
    "gold.fact_trip_detailed",
    "gold.fact_daily_zone_summary",
    "gold.fact_hourly_demand",
    "gold.fact_revenue_decomposition",
    "silver.trip_enriched",
    "bronze.trip_records",
]

def get_spark():
    return (
        SparkSession.builder
        .appName("iceberg_maintenance")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hive")
        .config("spark.sql.catalog.lake.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "s3a://nyc-tlc/warehouse")
        .getOrCreate()
    )

def call(spark, sql):
    print(f"[Iceberg] {sql}")
    spark.sql(sql)

def apply_table_properties(spark, fqtn: str, target_size_mb: int, sort_order: str | None):
    props = {
        "write.target-file-size-bytes": str(target_size_mb * 1024 * 1024),
        # setting write.order may help the compaction apply the desired clustering
    }
    if sort_order:
        # Iceberg write.order-by (v2) e.g., "trip_date, pickup_datetime, pickup_location_key, dropoff_location_key"
        props["write.order-by"] = sort_order

    # Build ALTER TABLE ... SET TBLPROPERTIES
    sets = ", ".join([f"'{k}'='{v}'" for k, v in props.items()])
    call(spark, f"ALTER TABLE {fqtn} SET TBLPROPERTIES ({sets})")

def run_for_table(spark, fqtn: str, older_than_days: int, retain_last: int, target_size_mb: int, min_input_files: int):
    # Apply desired properties (target size + write order if known)
    sort_order = None
    if fqtn.endswith("gold.fact_trip_detailed"):
        sort_order = "trip_date, pickup_datetime, pickup_location_key, dropoff_location_key"
    elif fqtn.endswith("silver.trip_enriched"):
        sort_order = "trip_date, pickup_datetime, PULocationID, DOLocationID"
    elif fqtn.endswith("bronze.trip_records"):
        sort_order = "trip_date, pickup_datetime"

    apply_table_properties(spark, fqtn, target_size_mb, sort_order)

    # 1) Rewrite data files (compaction + apply write order)
    call(
        spark,
        f"""
        CALL {CATALOG}.system.rewrite_data_files(
          table => '{fqtn}',
          options => map(
            'min-input-files','{min_input_files}',
            'target-file-size-bytes','{target_size_mb * 1024 * 1024}'
          )
        )
        """
    )

    # 2) Rewrite manifests (reduce small manifest overhead)
    call(
        spark,
        f"""
        CALL {CATALOG}.system.rewrite_manifests(
          table => '{fqtn}'
        )
        """
    )

    # 3) Expire old snapshots (keep last N + older-than cutoff)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S")
    call(
        spark,
        f"""
        CALL {CATALOG}.system.expire_snapshots(
          table => '{fqtn}',
          older_than => TIMESTAMP '{cutoff}',
          retain_last => {retain_last}
        )
        """
    )

def main():
    parser = argparse.ArgumentParser(description="Iceberg maintenance")
    parser.add_argument("--tables", type=str, default=",".join(DEFAULT_TABLES),
                        help="Comma-separated list of schema.table under catalog 'lake'")
    parser.add_argument("--older-than-days", type=int, default=30, help="Expire snapshots older than N days")
    parser.add_argument("--retain-last", type=int, default=10, help="Always keep last N snapshots")
    parser.add_argument("--target-file-size-mb", type=int, default=256, help="Compaction target file size MB")
    parser.add_argument("--min-input-files", type=int, default=5, help="Only compact when at least this many files can be rewritten")
    args = parser.parse_args()

    spark = get_spark()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    for t in tables:
        fqtn = f"{CATALOG}.{t}" if not t.startswith(f"{CATALOG}.") else t
        print(f"=== Maintenance for {fqtn} ===")
        run_for_table(
            spark,
            fqtn=fqtn,
            older_than_days=args.older_than_days,
            retain_last=args.retain_last,
            target_size_mb=args.target_file_size_mb,
            min_input_files=args.min_input_files,
        )

    spark.stop()

if __name__ == "__main__":
    main()
