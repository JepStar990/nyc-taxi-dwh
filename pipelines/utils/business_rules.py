# pipelines/utils/business_rules.py
from pyspark.sql import functions as F

# Airport/Zone constants (TLC zones)
JFK = 132
LGA = 138
EWR = 1
VALID_ZONE_MIN = 1
VALID_ZONE_MAX = 265  # 265 = Outside NYC

# Reasonable bounds
MIN_TRIP_SECS = 60               # >= 1 minute
MAX_TRIP_SECS = 24 * 3600        # <= 24 hours
MAX_SPEED_MPH = 80.0             # practical upper bound for city trips
TOTAL_TOLERANCE = 1.00           # $1.00 tolerance for component sum vs total

PEAK_HOURS = [7,8,9,16,17,18]

def add_derived_fields(df):
    """
    Adds derived columns: duration, speed, fare_per_mile, tip_percentage, flags.
    Assumes df has canonical Bronze columns.
    """
    # Duration & speed
    df = (df
        .withColumn("trip_duration_seconds",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")).cast("int"))
        .withColumn("trip_speed_mph",
            F.when(F.col("trip_duration_seconds") > 0,
                   (F.col("trip_distance") / (F.col("trip_duration_seconds")/3600.0)))
             .otherwise(None))
        .withColumn("fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount")/F.col("trip_distance")).otherwise(None))
        .withColumn("tip_percentage",
            F.when((F.col("fare_amount") > 0) & (F.col("tip_amount") >= 0),
                   (F.col("tip_amount") / F.col("fare_amount")) * 100.0)
             .otherwise(0.0))
        .withColumn("is_peak_hour", F.hour("pickup_datetime").isin(PEAK_HOURS))
    )

    # Shared trip best-effort (HVFHS flags only)
    df = df.withColumn(
        "is_shared_trip",
        (F.coalesce(F.col("shared_match_flag"), F.lit("N")) == F.lit("Y"))
        | (F.coalesce(F.col("shared_request_flag"), F.lit("N")) == F.lit("Y"))
    )

    # Airport
    df = df.withColumn(
        "is_airport_trip",
        F.col("PULocationID").isin(JFK, LGA, EWR) | F.col("DOLocationID").isin(JFK, LGA, EWR) |
        F.col("RatecodeID").isin(2,3) # JFK/Newark codes (TLC)
    )

    # Zero-distance fare pattern
    df = df.withColumn(
        "is_zero_distance_fare",
        (F.col("trip_distance") <= 0.01) & (F.coalesce(F.col("fare_amount"), F.lit(0.0)) >= 3.0)
    )

    return df


def add_validation_flags(df):
    """
    Adds boolean validation flags for temporal, spatial, and financial consistency.
    """
    monetary_cols = [
        "fare_amount","extra","mta_tax","improvement_surcharge",
        "tip_amount","tolls_amount","congestion_surcharge",
        "airport_fee","ehail_fee","cbd_congestion_fee"
    ]

    # Null-safe coalesce for sums
    computed_total = None
    for c in monetary_cols:
        col_expr = F.coalesce(F.col(c), F.lit(0.0))
        computed_total = col_expr if computed_total is None else (computed_total + col_expr)

    df = (df
        # Temporal validity
        .withColumn("v_time_order", F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .withColumn("v_duration_min", F.col("trip_duration_seconds") >= F.lit(MIN_TRIP_SECS))
        .withColumn("v_duration_max", F.col("trip_duration_seconds") <= F.lit(MAX_TRIP_SECS))
        # Spatial validity
        .withColumn("v_pu_zone", F.col("PULocationID").between(VALID_ZONE_MIN, VALID_ZONE_MAX))
        .withColumn("v_do_zone", F.col("DOLocationID").between(VALID_ZONE_MIN, VALID_ZONE_MAX))
        # Financial validity
        .withColumn("computed_total", computed_total)
        .withColumn("v_total_tolerance",
            F.abs(F.coalesce(F.col("total_amount"), F.lit(0.0)) - F.col("computed_total")) <= F.lit(TOTAL_TOLERANCE))
        .withColumn("v_non_negative_amounts",
            F.least(*[F.coalesce(F.col(c), F.lit(0.0)) for c in ["fare_amount","extra","mta_tax",
                                                                  "improvement_surcharge","tip_amount",
                                                                  "tolls_amount","congestion_surcharge",
                                                                  "airport_fee","ehail_fee","cbd_congestion_fee"]]) >= 0.0)
        .withColumn("v_tip_logic",
            (F.coalesce(F.col("tip_amount"), F.lit(0.0)) <=
             (F.coalesce(F.col("total_amount"), F.lit(0.0)) - F.coalesce(F.col("tolls_amount"), F.lit(0.0))))
        )
        # Business validity
        .withColumn("v_speed_reasonable",
            (F.col("trip_speed_mph").isNull()) | (F.col("trip_speed_mph") <= F.lit(MAX_SPEED_MPH)))
        .withColumn("v_distance_nonneg", F.coalesce(F.col("trip_distance"), F.lit(0.0)) >= 0.0)
        .withColumn("v_airport_ratecode",
            F.when(F.col("is_airport_trip"),
                   F.col("RatecodeID").isin(2,3) | F.col("service_type").isin("hvfhs","fhv"))  # HVFHS/FHV not metered
             .otherwise(F.lit(True)))
    )

    return df


def add_quality_score_and_flags(df):
    """
    Combine validations to a data_quality_score (0..100) and suspicious flag.
    """
    # Scoring heuristic (sum of booleans * weights = 0..100)
    df = df.withColumn("score_time",
                       (F.col("v_time_order") & F.col("v_duration_min") & F.col("v_duration_max")).cast("int") * F.lit(30)) \
           .withColumn("score_space",
                       (F.col("v_pu_zone") & F.col("v_do_zone")).cast("int") * F.lit(15)) \
           .withColumn("score_fin_total",
                       (F.col("v_total_tolerance")).cast("int") * F.lit(25)) \
           .withColumn("score_fin_nonneg",
                       (F.col("v_non_negative_amounts")).cast("int") * F.lit(10)) \
           .withColumn("score_tip_logic",
                       (F.col("v_tip_logic")).cast("int") * F.lit(10)) \
           .withColumn("score_speed_dist",
                       (F.col("v_speed_reasonable") & F.col("v_distance_nonneg")).cast("int") * F.lit(10))

    df = df.withColumn(
            "data_quality_score",
            F.col("score_time") + F.col("score_space") + F.col("score_fin_total") +
            F.col("score_fin_nonneg") + F.col("score_tip_logic") + F.col("score_speed_dist")
        ).drop("score_time","score_space","score_fin_total","score_fin_nonneg","score_tip_logic","score_speed_dist")

    # Suspicious patterns (kept but flagged)
    df = df.withColumn(
        "is_suspicious",
        (F.col("is_zero_distance_fare")) |
        (F.col("tip_percentage") > 60.0) |
        (F.col("trip_speed_mph") > 65.0)
    )

    # Composite pass flag (strict)
    df = df.withColumn(
        "dq_passed",
        F.col("v_time_order") & F.col("v_duration_min") & F.col("v_duration_max") &
        F.col("v_pu_zone") & F.col("v_do_zone") &
        F.col("v_total_tolerance") & F.col("v_non_negative_amounts") &
        F.col("v_tip_logic") & F.col("v_speed_reasonable") & F.col("v_distance_nonneg")
    )

    return df
