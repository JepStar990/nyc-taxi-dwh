# pipelines/utils/contracts.py
"""
Schema-by-era contract validation for NYC TLC datasets.
This checks RAW tables (Iceberg) BEFORE we proceed to Bronze/Silver/Gold.

Logic:
- Determine the "era" by pickup date:
    * Yellow/Green:
        - v2015-2018
        - v2019-2024
        - v2025+ (includes cbd_congestion_fee)
    * HVFHS: v2019+ (post law effective 2019-02-01)
    * FHV:   v_all
- Validate presence/absence of columns for the era and basic types.
- Persist results in lake.raw._contract_results and optionally raise on failure.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F

# --- Column contracts (name-only checks; types are cast in Bronze) ---

YELLOW_V2015_2018 = {
    "tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID",
    "passenger_count","trip_distance","fare_amount","extra","mta_tax",
    "tip_amount","tolls_amount","improvement_surcharge","payment_type",
    "RatecodeID","store_and_fwd_flag","VendorID","total_amount"
}

YELLOW_V2019_2024 = YELLOW_V2015_2018.union({"congestion_surcharge","airport_fee"})
YELLOW_V2025_PLUS = YELLOW_V2019_2024.union({"cbd_congestion_fee"})

GREEN_V2015_2018 = {
    "lpep_pickup_datetime","lpep_dropoff_datetime","PULocationID","DOLocationID",
    "passenger_count","trip_distance","fare_amount","extra","mta_tax",
    "tip_amount","tolls_amount","improvement_surcharge","payment_type",
    "RatecodeID","store_and_fwd_flag","VendorID","trip_type","total_amount"
}

GREEN_V2019_2024 = GREEN_V2015_2018.union({"congestion_surcharge","airport_fee"})
GREEN_V2025_PLUS = GREEN_V2019_2024.union({"cbd_congestion_fee"})

HVFHS_V2019_PLUS = {
    "hvfhs_license_num","request_datetime","pickup_datetime","dropoff_datetime",
    "PULocationID","DOLocationID","trip_miles","trip_time","base_passenger_fare",
    "tolls","bcf","sales_tax","congestion_surcharge","airport_fee","tips",
    "driver_pay","shared_request_flag","shared_match_flag","cbd_congestion_fee"
}

FHV_V_ALL = {
    "pickup_datetime","dropOff_datetime","PUlocationID","DOlocationID","SR_Flag","dispatching_base_num","Affiliated_base_number"
}

# --- Helpers ---

def _era_for_yellow(min_dt: str) -> str:
    if min_dt >= "2025-01-05":  # CBD fee effective date window (operationalized)
        return "v2025+"
    elif min_dt >= "2019-01-01":
        return "v2019-2024"
    else:
        return "v2015-2018"

def _era_for_green(min_dt: str) -> str:
    if min_dt >= "2025-01-05":
        return "v2025+"
    elif min_dt >= "2019-01-01":
        return "v2019-2024"
    else:
        return "v2015-2018"

def _era_for_hvfhs(min_dt: str) -> str:
    return "v2019+"

def _era_for_fhv(min_dt: str) -> str:
    return "v_all"

def _expected_cols(service: str, era: str) -> set:
    if service == "yellow":
        return {"v2015-2018":YELLOW_V2015_2018,"v2019-2024":YELLOW_V2019_2024,"v2025+":YELLOW_V2025_PLUS}[era]
    if service == "green":
        return {"v2015-2018":GREEN_V2015_2018,"v2019-2024":GREEN_V2019_2024,"v2025+":GREEN_V2025_PLUS}[era]
    if service == "hvfhs":
        return HVFHS_V2019_PLUS
    if service == "fhv":
        return FHV_V_ALL
    return set()

def _pick_min_pickup_col(service: str):
    # Which pickup col we use to infer era
    return {
        "yellow":"tpep_pickup_datetime",
        "green":"lpep_pickup_datetime",
        "hvfhs":"pickup_datetime",
        "fhv":"pickup_datetime"
    }[service]

def _infer_era(service: str, min_dt_str: str) -> str:
    if service == "yellow": return _era_for_yellow(min_dt_str)
    if service == "green":  return _era_for_green(min_dt_str)
    if service == "hvfhs":  return _era_for_hvfhs(min_dt_str)
    if service == "fhv":    return _era_for_fhv(min_dt_str)
    return "unknown"

def validate_raw_table(spark: SparkSession, table: str, service: str):
    df = spark.table(table)
    if df.rdd.isEmpty():
        return {
            "service": service, "table": table, "era": "n/a",
            "records": 0, "missing_cols": "", "extra_cols": "", "status": "EMPTY"
        }

    # era by min pickup
    pickup_col = _pick_min_pickup_col(service)
    if pickup_col not in df.columns:
        # fall back to created/ingest; consider violation
        cols = set(df.columns)
        return {
            "service": service, "table": table, "era": "unknown",
            "records": df.count(),
            "missing_cols": pickup_col, "extra_cols": ",".join(sorted(list(cols))),
            "status": "FAIL_NO_PICKUP_COL"
        }

    min_dt_row = df.select(F.date_format(F.to_timestamp(F.col(pickup_col)), "yyyy-MM-dd").alias("d")).agg(F.min("d")).first()
    min_dt = min_dt_row[0] if min_dt_row and min_dt_row[0] else "1900-01-01"
    era = _infer_era(service, min_dt)
    expected = _expected_cols(service, era)

    cols = set(df.columns)
    missing = sorted(list(expected - cols))
    extra   = sorted(list(cols - expected))

    status = "PASS" if not missing else "FAIL_MISSING_COLS"
    return {
        "service": service, "table": table, "era": era, "records": df.count(),
        "missing_cols": ",".join(missing), "extra_cols": ",".join(extra), "status": status
    }

def persist_results(spark: SparkSession, rows: list):
    out = spark.createDataFrame(rows, schema="""
        service STRING, table STRING, era STRING, records BIGINT,
        missing_cols STRING, extra_cols STRING, status STRING
    """)
    spark.sql("CREATE TABLE IF NOT EXISTS lake.raw._contract_results (service STRING, table STRING, era STRING, records BIGINT, missing_cols STRING, extra_cols STRING, status STRING) USING iceberg")
    out.writeTo("lake.raw._contract_results").append()

def validate_all_raw(spark: SparkSession, fail_on_violation: bool = True) -> None:
    checks = [
        ("lake.raw.yellow", "yellow"),
        ("lake.raw.green",  "green"),
        ("lake.raw.hvfhs",  "hvfhs"),
        ("lake.raw.fhv",    "fhv"),
    ]
    rows = [validate_raw_table(spark, t, s) for t, s in checks]
    persist_results(spark, rows)

    violations = [r for r in rows if r["status"] != "PASS" and r["status"] != "EMPTY"]
    if violations and fail_on_violation:
        raise RuntimeError(f"Contract validation failed: {violations}")
