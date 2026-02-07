# Exports selected Gold tables as Parquet snapshots under s3a://nyc-tlc/shares/<table>/snapshot_ts=YYYYMMDDHHMM
# Usage:
#   ./scripts/spark_submit_local.sh pipelines/sharing/export_gold_shares.py --tables fact_daily_zone_summary,fact_revenue_decomposition
import argparse, datetime
from pyspark.sql import SparkSession

DEFAULT = ["lake.gold.fact_daily_zone_summary","lake.gold.fact_revenue_decomposition"]

def spark():
    return (
        SparkSession.builder.appName("export_gold_shares")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type","hive")
        .config("spark.sql.catalog.lake.uri","thrift://hive-metastore:9083")
        .getOrCreate()
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tables", type=str, default=",".join(DEFAULT))
    args = p.parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M")

    sp = spark()
    for t in tables:
        df = sp.table(t)
        short = t.split(".")[-1]
        out = f"s3a://nyc-tlc/shares/{short}/snapshot_ts={ts}"
        print(f"[SHARE] Writing snapshot for {t} -> {out}")
        (df.coalesce(1).write.mode("overwrite").parquet(out))
    sp.stop()

if __name__ == "__main__":
    main()
