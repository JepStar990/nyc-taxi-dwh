# pipelines/utils/dq.py
from pyspark.sql import functions as F

DQ_TABLE = "lake.silver._dq_results"

DQ_RULE_COLUMNS = [
    "v_time_order","v_duration_min","v_duration_max",
    "v_pu_zone","v_do_zone",
    "v_total_tolerance","v_non_negative_amounts","v_tip_logic",
    "v_speed_reasonable","v_distance_nonneg","v_airport_ratecode"
]

def create_dq_table_if_missing(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DQ_TABLE} (
            run_ts TIMESTAMP,
            rule_name STRING,
            total BIGINT,
            passes BIGINT,
            fails BIGINT,
            pass_rate DOUBLE
        ) USING iceberg
    """)

def materialize_dq_results(spark, df, run_ts_col="bronze_ingest_time"):
    """
    Aggregates rule pass/fail counts and writes to the DQ Iceberg table.
    Expects df to contain DQ boolean columns and a timestamp column for run_ts.
    """
    create_dq_table_if_missing(spark)

    # Use first non-null timestamp column as run timestamp; fallback to current_timestamp
    df_with_ts = df.withColumn(
        "_run_ts",
        F.coalesce(F.col(run_ts_col), F.current_timestamp())
    )

    rows = []
    total = df_with_ts.count()
    for rule in DQ_RULE_COLUMNS:
        if rule in df_with_ts.columns:
            passes = df_with_ts.filter(F.col(rule) == True).count()
            fails = total - passes
            pass_rate = 0.0 if total == 0 else passes/total
            rows.append((rule, total, passes, fails, pass_rate))

    # Build DataFrame and append
    if rows:
        res_df = spark.createDataFrame(rows, ["rule_name","total","passes","fails","pass_rate"]) \
                      .withColumn("run_ts", F.current_timestamp()) \
                      .select("run_ts","rule_name","total","passes","fails","pass_rate")
        (res_df.writeTo(DQ_TABLE).append())
