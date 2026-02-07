# pipelines/transform/bronze_to_silver.py
# Silver: Clean, validate, and enrich Bronze into analytics-ready trip records.
# Usage:
#   ./scripts/spark_submit_local.sh pipelines/transform/bronze_to_silver.py

from pyspark.sql import functions as F
from pipelines.utils.spark_session import get_spark
from pipelines.utils.business_rules import (
    add_derived_fields, add_validation_flags, add_quality_score_and_flags
)
from pipelines.utils.dq import materialize_dq_results

SRC = "lake.bronze.trip_records"
DST = "lake.silver.trip_enriched"

def create_silver_table_if_missing(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DST} (
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

          -- Derived / Silver fields
          trip_duration_seconds INT,
          trip_speed_mph DOUBLE,
          fare_per_mile DOUBLE,
          tip_percentage DOUBLE,
          is_shared_trip BOOLEAN,
          is_airport_trip BOOLEAN,
          is_zero_distance_fare BOOLEAN,
          is_peak_hour BOOLEAN,

          -- Validation flags
          v_time_order BOOLEAN,
          v_duration_min BOOLEAN,
          v_duration_max BOOLEAN,
          v_pu_zone BOOLEAN,
          v_do_zone BOOLEAN,
          v_total_tolerance BOOLEAN,
          v_non_negative_amounts BOOLEAN,
          v_tip_logic BOOLEAN,
          v_speed_reasonable BOOLEAN,
          v_distance_nonneg BOOLEAN,
          v_airport_ratecode BOOLEAN,

          -- Quality
          data_quality_score INT,
          is_suspicious BOOLEAN,
          dq_passed BOOLEAN,

          bronze_ingest_time TIMESTAMP,
          source_table STRING,
          load_id STRING,
          trip_date DATE
        )
        USING iceberg
        PARTITIONED BY (trip_date)
    """)

def main():
    spark = get_spark("silver_trip_enrichment")
    create_silver_table_if_missing(spark)

    df = spark.table(SRC)

    # 1) Derived fields
    df = add_derived_fields(df)

    # 2) Validation flags
    df = add_validation_flags(df)

    # 3) Score + suspicious + pass flag
    df = add_quality_score_and_flags(df)

    # 4) Filter out records that fail hard constraints (Silver keeps valid; suspicious kept & flagged)
    df_valid = df.filter(F.col("dq_passed") == True)

    # 5) Write to Iceberg (replace for idempotent dev runs)
    (df_valid
        .writeTo(DST)
        .using("iceberg")
        .tableProperty("format-version","2")
        .tableProperty("write.format.default","parquet")
        .createOrReplace())

    # 6) Emit DQ metrics to a table for dashboards
    materialize_dq_results(spark, df)

    # Optional: quick counts
    df_valid.groupBy("service_type").count().orderBy("service_type").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
