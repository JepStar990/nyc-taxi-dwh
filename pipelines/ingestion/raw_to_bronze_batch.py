# pipelines/ingestion/raw_to_bronze_batch.py
# Standardize RAW datasets into one Bronze table: lake.bronze.trip_records
# Usage (from repo root):
#   ./scripts/spark_submit_local.sh pipelines/ingestion/raw_to_bronze_batch.py

import uuid
from pyspark.sql import functions as F, types as T
from pipelines.utils.spark_session import get_spark

BRONZE_TABLE = "lake.bronze.trip_records"

# ---------------------------
# Helpers to standardize dfs
# ---------------------------
def with_common_columns(df, service_type: str, source_table: str):
    """
    Ensure a complete set of Bronze columns exists and cast to canonical types.
    Missing columns are added as NULLs.
    """
    # Canonical Bronze schema (wide to accommodate all sources)
    cols = {
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
        "trip_date": T.DateType(),
    }

    # add metadata now
    df = (
        df
        .withColumn("service_type", F.lit(service_type))
        .withColumn("source_table", F.lit(source_table))
        .withColumn("bronze_ingest_time", F.current_timestamp())
        .withColumn("load_id", F.lit(str(uuid.uuid4())))
    )

    # ensure all columns exist with correct types
    for name, dtype in cols.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
        else:
            df = df.withColumn(name, F.col(name).cast(dtype))

    # derive trip_date (from pickup_datetime if present)
    df = df.withColumn(
        "trip_date",
        F.when(F.col("pickup_datetime").isNotNull(), F.to_date("pickup_datetime"))
         .otherwise(F.to_date(F.current_timestamp()))
    )
    return df.select(*cols.keys())


def transform_yellow(df):
    # Normalize column names/types to canonical fields
    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        .withColumnRenamed("PULocationID", "PULocationID")
        .withColumnRenamed("DOLocationID", "DOLocationID")
        .withColumnRenamed("VendorID", "VendorID")
        .withColumnRenamed("RatecodeID", "RatecodeID")
        .withColumnRenamed("payment_type", "payment_type")
        .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")
        .withColumnRenamed("passenger_count", "passenger_count")
        .withColumnRenamed("trip_distance", "trip_distance")
        .withColumnRenamed("fare_amount", "fare_amount")
        .withColumnRenamed("extra", "extra")
        .withColumnRenamed("mta_tax", "mta_tax")
        .withColumnRenamed("tip_amount", "tip_amount")
        .withColumnRenamed("tolls_amount", "tolls_amount")
    )
    # Some columns may not exist (depending on era), guard with coalesce to preserve nulls
    for c in ["improvement_surcharge","congestion_surcharge","airport_fee","ehail_fee","cbd_congestion_fee","total_amount"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    return df


def transform_green(df):
    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("lpep_pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("lpep_dropoff_datetime"))
        .withColumnRenamed("PULocationID", "PULocationID")
        .withColumnRenamed("DOLocationID", "DOLocationID")
        .withColumnRenamed("VendorID", "VendorID")
        .withColumnRenamed("RatecodeID", "RatecodeID")
        .withColumnRenamed("payment_type", "payment_type")
        .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")
        .withColumnRenamed("passenger_count", "passenger_count")
        .withColumnRenamed("trip_distance", "trip_distance")
        .withColumnRenamed("fare_amount", "fare_amount")
        .withColumnRenamed("extra", "extra")
        .withColumnRenamed("mta_tax", "mta_tax")
        .withColumnRenamed("tip_amount", "tip_amount")
        .withColumnRenamed("tolls_amount", "tolls_amount")
    )
    # trip_type exists on green only
    if "trip_type" in df.columns:
        df = df.withColumnRenamed("trip_type", "trip_type")
    else:
        df = df.withColumn("trip_type", F.lit(None))

    for c in ["improvement_surcharge","congestion_surcharge","airport_fee","ehail_fee","cbd_congestion_fee","total_amount"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    return df


def transform_fhv(df):
    # FHV has fewer fields; align what we can
    pu = "PUlocationID" if "PUlocationID" in df.columns else "PULocationID"
    do = "DOlocationID" if "DOlocationID" in df.columns else "DOLocationID"

    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp(F.coalesce("dropOff_datetime","dropoff_datetime")))
    )
    for colname, target in [(pu,"PULocationID"), (do,"DOLocationID")]:
        if colname in df.columns:
            df = df.withColumnRenamed(colname, target)
        else:
            df = df.withColumn(target, F.lit(None))

    # No fares/distances in FHV raw; keep nulls
    needed_nulls = [
        "passenger_count","trip_distance","fare_amount","extra","mta_tax","improvement_surcharge",
        "tip_amount","tolls_amount","congestion_surcharge","airport_fee","ehail_fee",
        "cbd_congestion_fee","total_amount","payment_type","RatecodeID","store_and_fwd_flag",
        "VendorID","trip_type","hvfhs_license_num","driver_pay","shared_request_flag","shared_match_flag",
        "bcf","sales_tax","base_passenger_fare","request_datetime"
    ]
    for c in needed_nulls:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    return df


def transform_hvfhs(df):
    # Map HVFHS fields to canonical
    df = (
        df
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        .withColumn("request_datetime", F.to_timestamp("request_datetime"))
    )

    # Location IDs (already named PULocationID/DOLocationID in HVFHS)
    for c in ["PULocationID","DOLocationID"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    # Distances: trip_miles -> trip_distance
    if "trip_miles" in df.columns:
        df = df.withColumn("trip_distance", F.col("trip_miles").cast("double"))
    else:
        df = df.withColumn("trip_distance", F.lit(None).cast("double"))

    # Financials: compute total_amount since HVFHS doesn't provide directly
    for c in ["base_passenger_fare","tolls","congestion_surcharge","airport_fee","sales_tax","bcf","tips","driver_pay","cbd_congestion_fee"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(0.0))

    df = (
        df
        .withColumn("fare_amount", F.col("base_passenger_fare").cast("double"))
        .withColumn("tolls_amount", F.col("tolls").cast("double"))
        .withColumn("tip_amount", F.col("tips").cast("double"))
        .withColumn("driver_pay", F.col("driver_pay").cast("double"))
        .withColumn("bcf", F.col("bcf").cast("double"))
        .withColumn("sales_tax", F.col("sales_tax").cast("double"))
        .withColumn("cbd_congestion_fee", F.col("cbd_congestion_fee").cast("double"))
        .withColumn(
            "total_amount",
            (F.col("base_passenger_fare") + F.col("tolls") + F.col("congestion_surcharge")
             + F.col("airport_fee") + F.col("sales_tax") + F.col("bcf") + F.col("tips")).cast("double")
        )
    )

    # Flags & license
    if "hvfhs_license_num" not in df.columns:
        df = df.withColumn("hvfhs_license_num", F.lit(None))
    if "shared_request_flag" not in df.columns:
        df = df.withColumn("shared_request_flag", F.lit(None))
    if "shared_match_flag" not in df.columns:
        df = df.withColumn("shared_match_flag", F.lit(None))

    # Fill missing optional Taxi columns with NULL
    for c in ["extra","mta_tax","improvement_surcharge","payment_type","RatecodeID","store_and_fwd_flag","VendorID","airport_fee","ehail_fee"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    return df


def create_bronze_table_if_missing(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.bronze.trip_records (
          service_type STRING,
          pickup_datetime TIMESTAMP,
          dropoff_datetime TIMESTAMP,
          request_datetime TIMESTAMP,
          PULocationID INT,
          DOLocationID INT,
          passenger_count INT,
          trip_distance DOUBLE,
          fare_amount DOUBLE,
          extra DOUBLE,
          mta_tax DOUBLE,
          improvement_surcharge DOUBLE,
          tip_amount DOUBLE,
          tolls_amount DOUBLE,
          congestion_surcharge DOUBLE,
          airport_fee DOUBLE,
          ehail_fee DOUBLE,
          cbd_congestion_fee DOUBLE,
          total_amount DOUBLE,
          payment_type INT,
          RatecodeID INT,
          store_and_fwd_flag STRING,
          VendorID INT,
          trip_type INT,
          hvfhs_license_num STRING,
          driver_pay DOUBLE,
          shared_request_flag STRING,
          shared_match_flag STRING,
          bcf DOUBLE,
          sales_tax DOUBLE,
          base_passenger_fare DOUBLE,
          bronze_ingest_time TIMESTAMP,
          source_table STRING,
          load_id STRING,
          trip_date DATE
        )
        USING iceberg
        PARTITIONED BY (trip_date)
    """)

def main():
    spark = get_spark("bronze_raw_standardization")

    # Ensure the Bronze table exists
    create_bronze_table_if_missing(spark)

    # Read RAW tables (created in Phase 1)
    raw = {
        "yellow": spark.table("lake.raw.yellow"),
        "green":  spark.table("lake.raw.green"),
        "hvfhs":  spark.table("lake.raw.hvfhs"),
        "fhv":    spark.table("lake.raw.fhv")
    }

    # Transform each dataset to canonical structure
    y = with_common_columns(transform_yellow(raw["yellow"]), "yellow", "lake.raw.yellow")
    g = with_common_columns(transform_green(raw["green"]),   "green",  "lake.raw.green")
    h = with_common_columns(transform_hvfhs(raw["hvfhs"]),   "hvfhs",  "lake.raw.hvfhs")
    f = with_common_columns(transform_fhv(raw["fhv"]),       "fhv",    "lake.raw.fhv")

    # Union and write to Bronze
    bronze = y.unionByName(g, allowMissingColumns=True)\
              .unionByName(h, allowMissingColumns=True)\
              .unionByName(f, allowMissingColumns=True)

    # OPTIONAL sanity filters (drop obvious junk)
    bronze = bronze.filter(F.col("pickup_datetime").isNotNull())

    (bronze
     .writeTo(BRONZE_TABLE)
     .append())

    # Quick KPI logs (optional)
    counts = bronze.groupBy("service_type").count().orderBy("service_type").collect()
    for row in counts:
        print(f"[BRONZE] {row['service_type']}: {row['count']} rows")

    spark.stop()

if __name__ == "__main__":
    main()
