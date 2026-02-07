# pipelines/transform/silver_to_gold.py
import argparse
import os
from pyspark.sql import functions as F, types as T, Window
from pipelines.utils.spark_session import get_spark

GOLD = "lake.gold"
SILVER = "lake.silver"

DDL_FILE = "/workspace/sql/ddl/star_schema.sql"

def run_sql_file(spark, path):
    with open(path, "r") as f:
        sql_text = f.read()
    for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
        spark.sql(stmt)

def ensure_gold_schema(spark):
    run_sql_file(spark, DDL_FILE)

# -----------------------------
# Dimensions
# -----------------------------
def upsert_dim_vendor(spark):
    mapping = [
        (1, "Creative Mobile Technologies, LLC"),
        (2, "Curb Mobility, LLC"),
        (6, "Myle Technologies Inc"),
        (7, "Helix"),
    ]
    df = spark.createDataFrame(mapping, schema="vendor_id INT, vendor_name STRING") \
              .withColumn("vendor_key", F.col("vendor_id").cast("int")) \
              .select("vendor_key","vendor_id","vendor_name")
    df.createOrReplaceTempView("v_df")
    spark.sql(f"""
        MERGE INTO {GOLD}.dim_vendor t
        USING v_df s
        ON t.vendor_key = s.vendor_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_dim_payment_type(spark):
    mapping = [
        (0,"Flex Fare trip"), (1,"Credit card"), (2,"Cash"),
        (3,"No charge"), (4,"Dispute"), (5,"Unknown"), (6,"Voided trip")
    ]
    df = spark.createDataFrame(mapping, "payment_type_id INT, payment_type_desc STRING") \
              .withColumn("payment_type_key", F.col("payment_type_id"))
    df.createOrReplaceTempView("p_df")
    spark.sql(f"""
        MERGE INTO {GOLD}.dim_payment_type t
        USING p_df s
        ON t.payment_type_key = s.payment_type_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_dim_rate_code(spark):
    mapping = [
        (1,"Standard rate"), (2,"JFK"), (3,"Newark"),
        (4,"Nassau or Westchester"), (5,"Negotiated fare"),
        (6,"Group ride"), (99,"Null/unknown")
    ]
    df = spark.createDataFrame(mapping, "rate_code_id INT, rate_code_desc STRING") \
              .withColumn("rate_code_key", F.col("rate_code_id"))
    df.createOrReplaceTempView("r_df")
    spark.sql(f"""
        MERGE INTO {GOLD}.dim_rate_code t
        USING r_df s
        ON t.rate_code_key = s.rate_code_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_dim_trip_type(spark):
    mapping = [(1,"Street-hail"), (2,"Dispatch")]
    df = spark.createDataFrame(mapping, "trip_type_id INT, trip_type_desc STRING") \
              .withColumn("trip_type_key", F.col("trip_type_id"))
    df.createOrReplaceTempView("tt_df")
    spark.sql(f"""
        MERGE INTO {GOLD}.dim_trip_type t
        USING tt_df s
        ON t.trip_type_key = s.trip_type_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_dim_location_scd2(spark):
    # Seed from the CSV mirrored to MinIO in Phase 1
    zones_path = "s3a://nyc-tlc/raw/taxi_zone_lookup.csv"
    z = (spark.read.option("header", True).csv(zones_path)
          .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Zone").alias("zone"),
            F.col("Borough").alias("borough"),
            F.col("service_zone").alias("service_zone")
          ))

    # Current version SCD2 rows (valid forever)
    cur = (z
      .withColumn("zone_key", F.col("location_id"))
      .withColumn("borough_key", F.dense_rank().over(Window.orderBy("borough")))
      .withColumn("valid_from", F.lit("1900-01-01").cast("date"))
      .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
      .withColumn("is_current", F.lit(True))
      .withColumn("version", F.lit(1).cast("int"))
    )

    cur.createOrReplaceTempView("loc_cur")

    spark.sql(f"""
        MERGE INTO {GOLD}.dim_location_scd2 t
        USING loc_cur s
        ON t.location_id = s.location_id AND t.version = 1
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_dim_datetime_enhanced(spark, min_dt, max_dt):
    # Build hour grain series between min_dt and max_dt from Silver
    spark.sql("SET spark.sql.session.timeZone=UTC")
    series = (spark.range(0, 1)
              .select(F.sequence(F.to_timestamp(F.lit(min_dt)),
                                 F.to_timestamp(F.lit(max_dt)),
                                 F.expr('INTERVAL 1 HOUR')).alias("hours"))
              .selectExpr("explode(hours) as dt"))

    d = (series
        .withColumn("datetime_key", F.date_format("dt","yyyyMMddHH").cast("int"))
        .withColumn("date_key", F.date_format("dt","yyyyMMdd").cast("int"))
        .withColumn("year", F.year("dt"))
        .withColumn("month", F.month("dt"))
        .withColumn("day", F.dayofmonth("dt"))
        .withColumn("hour", F.hour("dt"))
        .withColumn("day_of_week", F.date_format("dt","u").cast("int"))  # 1..7
        .withColumn("is_weekend", F.col("day_of_week").isin(6,7))
        .withColumn("is_holiday", F.lit(False))       # hook up to a holiday table if available
        .withColumn("has_major_event", F.lit(False))  # hook up to events table if available
        .select("datetime_key","dt","date_key","year","month","day","hour",
                "day_of_week","is_weekend","is_holiday","has_major_event"))

    d.createOrReplaceTempView("dt_enh")

    spark.sql(f"""
        MERGE INTO {GOLD}.dim_datetime_enhanced t
        USING dt_enh s
        ON t.datetime_key = s.datetime_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# -----------------------------
# Fact + Aggregates
# -----------------------------
def build_fact_trip_detailed(spark, days: int = None, full_refresh: bool = False):
    silver = spark.table(f"{SILVER}.trip_enriched")

    if not full_refresh and days is not None:
        # Recompute a sliding window to capture late-arriving corrections
        silver = silver.filter(F.col("trip_date") >= F.date_sub(F.current_date(), days))

    # Dimension joins
    loc = spark.table(f"{GOLD}.dim_location_scd2").filter("is_current = true") \
            .select("location_id","zone_key","borough","borough_key")

    # Pickup join
    s = (silver.alias("s")
         .join(loc.alias("lpu"), F.col("s.PULocationID")==F.col("lpu.location_id"), "left")
         .join(loc.alias("ldo"), F.col("s.DOLocationID")==F.col("ldo.location_id"), "left")
    )

    # Vendor / payment / rate / trip type
    dv = spark.table(f"{GOLD}.dim_vendor").selectExpr("vendor_key as vendor_key_dim","vendor_id as vendor_id_dim")
    dp = spark.table(f"{GOLD}.dim_payment_type").selectExpr("payment_type_key as pt_key","payment_type_id as pt_id")
    dr = spark.table(f"{GOLD}.dim_rate_code").selectExpr("rate_code_key as rc_key","rate_code_id as rc_id")
    dt = spark.table(f"{GOLD}.dim_trip_type").selectExpr("trip_type_key as tt_key","trip_type_id as tt_id")

    s = (s
         .join(dv, F.col("s.VendorID")==F.col("vendor_id_dim"), "left")
         .join(dp, F.col("s.payment_type")==F.col("pt_id"), "left")
         .join(dr, F.col("s.RatecodeID")==F.col("rc_id"), "left")
         .join(dt, F.col("s.trip_type")==F.col("tt_id"), "left")
    )

    # Datetime keys
    def key(col): return F.date_format(F.col(col),"yyyyMMddHH").cast("int")
    s = (s
         .withColumn("pickup_datetime_key", key("s.pickup_datetime"))
         .withColumn("dropoff_datetime_key", key("s.dropoff_datetime"))
         .withColumn("request_datetime_key", F.when(F.col("s.request_datetime").isNotNull(), key("s.request_datetime")).otherwise(F.lit(None).cast("int")))
    )

    # Deterministic trip_id (handle duplicates and late updates)
    # Use a composite of service_type + pickup_datetime + PU/DO + Vendor + total_amount + load_id
    s = s.withColumn(
        "trip_id",
        F.sha1(F.concat_ws("||",
              F.coalesce(F.col("s.service_type"), F.lit("")),
              F.coalesce(F.col("s.pickup_datetime").cast("string"), F.lit("")),
              F.coalesce(F.col("s.PULocationID").cast("string"), F.lit("")),
              F.coalesce(F.col("s.DOLocationID").cast("string"), F.lit("")),
              F.coalesce(F.col("s.VendorID").cast("string"), F.lit("")),
              F.coalesce(F.col("s.total_amount").cast("string"), F.lit("")),
              F.coalesce(F.col("s.load_id"), F.lit(""))
        )))
    )

    fact_cols = [
      "trip_id",
      "s.pickup_datetime","s.dropoff_datetime","s.request_datetime",
      "pickup_datetime_key","dropoff_datetime_key","request_datetime_key",
      F.col("lpu.zone_key").alias("pickup_location_key"),
      F.col("ldo.zone_key").alias("dropoff_location_key"),
      F.col("lpu.borough_key").alias("pickup_borough_key"),
      F.col("ldo.borough_key").alias("dropoff_borough_key"),
      F.col("vendor_key_dim").alias("vendor_key"),
      F.col("pt_key").alias("payment_type_key"),
      F.col("rc_key").alias("rate_code_key"),
      F.col("tt_key").alias("trip_type_key"),
      "s.passenger_count","s.trip_distance","s.fare_amount",
      F.col("s.extra").alias("extra_amount"),
      F.col("s.mta_tax").alias("mta_tax_amount"),
      F.col("s.improvement_surcharge").alias("improvement_surcharge_amount"),
      "s.tip_amount","s.tolls_amount",
      F.col("s.congestion_surcharge").alias("congestion_surcharge_amount"),
      F.col("s.airport_fee").alias("airport_fee_amount"),
      F.col("s.ehail_fee").alias("ehail_fee_amount"),
      "s.cbd_congestion_fee","s.total_amount",
      "s.trip_duration_seconds","s.trip_speed_mph","s.tip_percentage","s.fare_per_mile",
      "s.is_shared_trip","s.is_airport_trip","s.is_peak_hour",
      "s.data_quality_score","s.is_suspicious",
      "s.service_type",
      "s.trip_date",
      F.current_timestamp().alias("created_ts"),
      F.current_timestamp().alias("modified_ts"),
      F.lit("silver.trip_enriched").alias("source_system"),
    ]

    fact_df = s.select(*[c if isinstance(c, str) else c for c in fact_cols])
    fact_df.createOrReplaceTempView("fact_stage")

    # Create a temp hashed subview limited to the processing window for MERGE
    spark.sql(f"""
        MERGE INTO {GOLD}.fact_trip_detailed t
        USING fact_stage s
        ON t.trip_id = s.trip_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def rebuild_fact_daily_zone_summary(spark, days=None, full_refresh=False):
    f = spark.table(f"{GOLD}.fact_trip_detailed")
    if not full_refresh and days is not None:
        f = f.filter(F.col("trip_date") >= F.date_sub(F.current_date(), days))

    agg = (f.groupBy("trip_date","pickup_location_key")
             .agg(F.count("*").alias("trips"),
                  F.sum("total_amount").alias("revenue"),
                  F.avg("trip_distance").alias("avg_miles"),
                  (F.avg(F.col("trip_duration_seconds"))/60.0).alias("avg_minutes"),
                  F.sum(F.col("is_suspicious").cast("int")).alias("suspicious_trips"))
             .withColumnRenamed("pickup_location_key","zone_key"))

    agg.createOrReplaceTempView("daily_zone_summary")

    spark.sql(f"""
        MERGE INTO {GOLD}.fact_daily_zone_summary t
        USING daily_zone_summary s
        ON t.trip_date = s.trip_date AND t.zone_key = s.zone_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def rebuild_fact_hourly_demand(spark, days=None, full_refresh=False):
    f = spark.table(f"{GOLD}.fact_trip_detailed")
    if not full_refresh and days is not None:
        f = f.filter(F.col("trip_date") >= F.date_sub(F.current_date(), days))

    # Join with datetime dim for event/holiday flags (optional)
    dd = spark.table(f"{GOLD}.dim_datetime_enhanced") \
              .select("datetime_key","has_major_event")
    agg = (f.select("pickup_datetime_key","pickup_location_key","trip_speed_mph","tip_percentage")
             .join(dd, "pickup_datetime_key", "left")
             .groupBy("pickup_datetime_key","pickup_location_key","has_major_event")
             .agg(F.count("*").alias("trips"),
                  F.avg("trip_speed_mph").alias("avg_speed"),
                  F.avg("tip_percentage").alias("avg_tip_pct"))
             .withColumnRenamed("pickup_location_key","zone_key"))

    agg.createOrReplaceTempView("hourly_demand")

    spark.sql(f"""
        MERGE INTO {GOLD}.fact_hourly_demand t
        USING hourly_demand s
        ON t.pickup_datetime_key = s.pickup_datetime_key AND t.zone_key = s.zone_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def rebuild_fact_revenue_decomposition(spark, days=None, full_refresh=False):
    f = spark.table(f"{GOLD}.fact_trip_detailed")
    if not full_refresh and days is not None:
        f = f.filter(F.col("trip_date") >= F.date_sub(F.current_date(), days))

    agg = (f.groupBy("trip_date","service_type")
             .agg(
                F.sum("fare_amount").alias("base_fare"),
                F.sum("extra_amount").alias("extras"),
                F.sum("mta_tax_amount").alias("mta_tax"),
                F.sum("improvement_surcharge_amount").alias("improvement_surcharge"),
                F.sum("tip_amount").alias("tips"),
                F.sum("tolls_amount").alias("tolls"),
                F.sum("congestion_surcharge_amount").alias("congestion"),
                F.sum("airport_fee_amount").alias("airport_fees"),
                F.sum("ehail_fee_amount").alias("ehail"),
                F.sum("cbd_congestion_fee").alias("cbd_fee"),
                F.sum("total_amount").alias("total")
             ))

    agg.createOrReplaceTempView("revenue_decomp")

    spark.sql(f"""
        MERGE INTO {GOLD}.fact_revenue_decomposition t
        USING revenue_decomp s
        ON t.trip_date = s.trip_date AND t.service_type = s.service_type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def parse_args():
    p = argparse.ArgumentParser(description="Silver -> Gold production builder")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--days", type=int, default=7, help="Incremental window in days (default 7)")
    g.add_argument("--full-refresh", action="store_true", help="Rebuild all (careful on large data)")
    return p.parse_args()

def main():
    args = parse_args()
    spark = get_spark("gold_star_schema")

    # 1) Ensure DDL is applied
    ensure_gold_schema(spark)

    # 2) Dimensions
    upsert_dim_vendor(spark)
    upsert_dim_payment_type(spark)
    upsert_dim_rate_code(spark)
    upsert_dim_trip_type(spark)
    upsert_dim_location_scd2(spark)

    # datetime dim window from Silver
    s = spark.table(f"{SILVER}.trip_enriched")
    if args.full_refresh:
        min_dt, max_dt = s.agg(F.min("pickup_datetime"), F.max("pickup_datetime")).first()
    else:
        s_win = s.filter(F.col("trip_date") >= F.date_sub(F.current_date(), args.days))
        min_dt, max_dt = s_win.agg(F.min("pickup_datetime"), F.max("pickup_datetime")).first()
        if min_dt is None or max_dt is None:
            # fallback to overall if window empty
            min_dt, max_dt = s.agg(F.min("pickup_datetime"), F.max("pickup_datetime")).first()

    upsert_dim_datetime_enhanced(spark, min_dt, max_dt)

    # 3) Fact upsert
    build_fact_trip_detailed(spark, days=args.days if not args.full_refresh else None, full_refresh=args.full_refresh)

    # 4) Aggregates
    rebuild_fact_daily_zone_summary(spark, days=args.days if not args.full_refresh else None, full_refresh=args.full_refresh)
    rebuild_fact_hourly_demand(spark, days=args.days if not args.full_refresh else None, full_refresh=args.full_refresh)
    rebuild_fact_revenue_decomposition(spark, days=args.days if not args.full_refresh else None, full_refresh=args.full_refresh)

    # 5) Maintenance hints (optional compact)
    # spark.sql("CALL lake.system.rewrite_data_files(table => 'lake.gold.fact_trip_detailed')")  # enable if needed

    spark.stop()

if __name__ == "__main__":
    main()
