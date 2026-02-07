# pipelines/ingestion/stream_to_bronze_spark.py
# Production streaming ingestion from Kafka → Bronze (Iceberg)
#
# Modes:
#   --continuous           : runs until stopped (for production)
#   --available-now        : micro-batch catch-up and exit
#   --watermark-minutes N  : event-time dedup watermark minutes (default 30)
#
# Usage:
#   ./scripts/run_stream_ingest.sh --continuous
#   ./scripts/run_stream_ingest.sh --available-now

import argparse
import os
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT_BASE = os.getenv("STREAM_CHECKPOINT_BASE", "s3a://nyc-tlc/checkpoints/bronze_stream")
DEST_TABLE = "lake.bronze.trip_records"

TOPICS = {
    "yellow": "yellow_trips",
    "green": "green_trips",
    "hvfhs": "hvfhs_trips"
}

def spark_session(app="stream_to_bronze"):
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type","hive")
        .config("spark.sql.catalog.lake.uri","thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir","s3a://nyc-tlc/warehouse")
        # S3A
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        # Exactly-once append via checkpointed micro-batches
        .config("spark.sql.shuffle.partitions","8")
        .getOrCreate()
    )

# ---------- JSON Schemas per topic ----------
yellow_schema = T.StructType([
    T.StructField("tpep_pickup_datetime", T.StringType()),
    T.StructField("tpep_dropoff_datetime", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
    T.StructField("airport_fee", T.DoubleType()),
    T.StructField("ehail_fee", T.DoubleType()),
    T.StructField("cbd_congestion_fee", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("payment_type", T.IntegerType()),
    T.StructField("RatecodeID", T.IntegerType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("VendorID", T.IntegerType())
])

green_schema = T.StructType([
    T.StructField("lpep_pickup_datetime", T.StringType()),
    T.StructField("lpep_dropoff_datetime", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
    T.StructField("airport_fee", T.DoubleType()),
    T.StructField("ehail_fee", T.DoubleType()),
    T.StructField("cbd_congestion_fee", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("payment_type", T.IntegerType()),
    T.StructField("RatecodeID", T.IntegerType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("VendorID", T.IntegerType()),
    T.StructField("trip_type", T.IntegerType())
])

hvfhs_schema = T.StructType([
    T.StructField("hvfhs_license_num", T.StringType()),
    T.StructField("request_datetime", T.StringType()),
    T.StructField("pickup_datetime", T.StringType()),
    T.StructField("dropoff_datetime", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("trip_miles", T.DoubleType()),
    T.StructField("trip_time", T.IntegerType()),
    T.StructField("base_passenger_fare", T.DoubleType()),
    T.StructField("tolls", T.DoubleType()),
    T.StructField("bcf", T.DoubleType()),
    T.StructField("sales_tax", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
    T.StructField("airport_fee", T.DoubleType()),
    T.StructField("tips", T.DoubleType()),
    T.StructField("driver_pay", T.DoubleType()),
    T.StructField("shared_request_flag", T.StringType()),
    T.StructField("shared_match_flag", T.StringType()),
    T.StructField("cbd_congestion_fee", T.DoubleType())
])

# ---------- Canonicalization helpers (align to Bronze) ----------
def canon_yellow(df):
    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    )
    # add missing fields if absent
    for c in ["improvement_surcharge","congestion_surcharge","airport_fee","ehail_fee","cbd_congestion_fee","total_amount"]:
        if c not in df.columns: df = df.withColumn(c, F.lit(None).cast("double"))
    return df

def canon_green(df):
    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("lpep_pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("lpep_dropoff_datetime"))
    )
    if "trip_type" not in df.columns:
        df = df.withColumn("trip_type", F.lit(None).cast("int"))
    for c in ["improvement_surcharge","congestion_surcharge","airport_fee","ehail_fee","cbd_congestion_fee","total_amount"]:
        if c not in df.columns: df = df.withColumn(c, F.lit(None).cast("double"))
    return df

def canon_hvfhs(df):
    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        .withColumn("request_datetime", F.to_timestamp("request_datetime"))
        .withColumn("trip_distance", F.col("trip_miles").cast("double"))
        .withColumn("fare_amount", F.col("base_passenger_fare").cast("double"))
        .withColumn("tolls_amount", F.col("tolls").cast("double"))
        .withColumn("tip_amount", F.col("tips").cast("double"))
    )
    for c in ["extra","mta_tax","improvement_surcharge","payment_type","RatecodeID","store_and_fwd_flag","VendorID","ehail_fee"]:
        if c not in df.columns: df = df.withColumn(c, F.lit(None))
    if "cbd_congestion_fee" not in df.columns: df = df.withColumn("cbd_congestion_fee", F.lit(0.0))
    if "airport_fee" not in df.columns: df = df.withColumn("airport_fee", F.lit(0.0))
    if "sales_tax" not in df.columns: df = df.withColumn("sales_tax", F.lit(0.0))
    if "bcf" not in df.columns: df = df.withColumn("bcf", F.lit(0.0))
    if "driver_pay" not in df.columns: df = df.withColumn("driver_pay", F.lit(0.0))
    if "total_amount" not in df.columns:
        df = df.withColumn(
            "total_amount",
            F.col("base_passenger_fare") + F.col("tolls") + F.col("congestion_surcharge") +
            F.col("airport_fee") + F.col("sales_tax") + F.col("bcf") + F.col("tips")
        )
    return df

def to_bronze_shape(df, service_type):
    # Canonical Bronze columns
    cols = [
        "service_type","pickup_datetime","dropoff_datetime","request_datetime",
        "PULocationID","DOLocationID","passenger_count","trip_distance",
        "fare_amount","extra","mta_tax","improvement_surcharge","tip_amount","tolls_amount",
        "congestion_surcharge","airport_fee","ehail_fee","cbd_congestion_fee","total_amount",
        "payment_type","RatecodeID","store_and_fwd_flag","VendorID","trip_type",
        "hvfhs_license_num","driver_pay","shared_request_flag","shared_match_flag","bcf","sales_tax",
        "base_passenger_fare","bronze_ingest_time","source_table","load_id","trip_date"
    ]
    df = (df
        .withColumn("service_type", F.lit(service_type))
        .withColumn("bronze_ingest_time", F.current_timestamp())
        .withColumn("source_table", F.lit(f"stream:{service_type}"))
        .withColumn("load_id", F.sha1(F.concat_ws("||",
            F.lit(service_type),
            F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("PULocationID").cast("string"), F.lit("")),
            F.coalesce(F.col("DOLocationID").cast("string"), F.lit("")),
            F.coalesce(F.col("total_amount").cast("string"), F.lit(""))
        )))
        .withColumn("trip_date", F.to_date("pickup_datetime"))
    )
    # ensure all columns exist
    schema_map = {
        "service_type": T.StringType(),
        "pickup_datetime": T.TimestampType(),
        "dropoff_datetime": T.TimestampType(),
        "request_datetime": T.TimestampType(),
        "PULocationID": T.IntegerType(),
        "DOLocationID": T.IntegerType(),
        "passenger_count": T.IntegerType(),
        "trip_distance": T.DoubleType(),
        "fare_amount": T.DoubleType(),
        "extra": T.DoubleType(),
        "mta_tax": T.DoubleType(),
        "improvement_surcharge": T.DoubleType(),
        "tip_amount": T.DoubleType(),
        "tolls_amount": T.DoubleType(),
        "congestion_surcharge": T.DoubleType(),
        "airport_fee": T.DoubleType(),
        "ehail_fee": T.DoubleType(),
        "cbd_congestion_fee": T.DoubleType(),
        "total_amount": T.DoubleType(),
        "payment_type": T.IntegerType(),
        "RatecodeID": T.IntegerType(),
        "store_and_fwd_flag": T.StringType(),
        "VendorID": T.IntegerType(),
        "trip_type": T.IntegerType(),
        "hvfhs_license_num": T.StringType(),
        "driver_pay": T.DoubleType(),
        "shared_request_flag": T.StringType(),
        "shared_match_flag": T.StringType(),
        "bcf": T.DoubleType(),
        "sales_tax": T.DoubleType(),
        "base_passenger_fare": T.DoubleType(),
        "bronze_ingest_time": T.TimestampType(),
        "source_table": T.StringType(),
        "load_id": T.StringType(),
        "trip_date": T.DateType()
    }
    for c, typ in schema_map.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(typ))
        else:
            df = df.withColumn(c, F.col(c).cast(typ))
    return df.select(*cols)

# ---------- Stream builders ----------
def build_stream(spark, topic, schema, canon_fn, service_type, watermark_minutes=30):
    src = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        src.select(F.col("timestamp").alias("_kafka_ts"),
                   F.from_json(F.col("value").cast("string"), schema).alias("j"))
           .select("._kafka_ts", "j.*")
    )
    shaped = canon_fn(parsed)
    bronze = to_bronze_shape(shaped, service_type)

    # Event-time dedup on (service_type, pickup_datetime, PU, DO, total_amount)
    bronze = (
        bronze
        .withWatermark("pickup_datetime", f"{watermark_minutes} minutes")
        .withColumn("_dupe_key", F.sha1(F.concat_ws("||",
            F.col("service_type"),
            F.col("pickup_datetime").cast("string"),
            F.col("PULocationID").cast("string"),
            F.col("DOLocationID").cast("string"),
            F.coalesce(F.col("total_amount").cast("string"), F.lit(""))
        )))
        .dropDuplicates(["_dupe_key"])
        .drop("_dupe_key")
    )
    return bronze

def foreach_batch_writer(df, epoch_id):
    # Append micro-batch to Bronze table (idempotent thanks to checkpointing)
    (df
     .writeTo(DEST_TABLE)
     .append())

def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--continuous", action="store_true", help="run until stopped")
    mode.add_argument("--available-now", action="store_true", help="process available data and exit")
    parser.add_argument("--watermark-minutes", type=int, default=int(os.getenv("STREAM_WATERMARK_MIN", "30")))
    args = parser.parse_args()

    spark = spark_session("stream_to_bronze")

    # Build three streams
    y = build_stream(spark, TOPICS["yellow"], yellow_schema, canon_yellow, "yellow", args.watermark_minutes)
    g = build_stream(spark, TOPICS["green"],  green_schema,  canon_green,  "green",  args.watermark_minutes)
    h = build_stream(spark, TOPICS["hvfhs"],  hvfhs_schema,  canon_hvfhs,  "hvfhs",  args.watermark_minutes)

    unified = y.unionByName(g, allowMissingColumns=True).unionByName(h, allowMissingColumns=True)

    writer = (unified.writeStream
              .foreachBatch(foreach_batch_writer)
              .option("checkpointLocation", f"{CHECKPOINT_BASE}/trip_records"))

    if args.available_now:
        (writer.trigger(availableNow=True).start().awaitTermination())
    else:
        (writer.trigger(processingTime="30 seconds").start().awaitTermination())

if __name__ == "__main__":
    main()
