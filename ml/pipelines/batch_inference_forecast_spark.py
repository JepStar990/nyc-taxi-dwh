# Produces next-hour predictions per zone using the trained model.
# Writes to lake.gold.pred_hourly_demand (MERGE by pickup_datetime_key, zone_key).
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.pipeline import PipelineModel

MODEL_URI = os.getenv("FORECAST_MODEL_URI", "s3a://nyc-tlc/models/forecast_gbt")

def spark():
    return (
        SparkSession.builder.appName("batch_inference_forecast")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type","hive")
        .config("spark.sql.catalog.lake.uri","thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir","s3a://nyc-tlc/warehouse")
        .getOrCreate()
    )

def ensure_table(sp):
    sp.sql("""
        CREATE TABLE IF NOT EXISTS lake.gold.pred_hourly_demand (
          pickup_datetime_key INT,
          zone_key INT,
          predicted_trips DOUBLE,
          model_uri STRING,
          created_ts TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (pickup_datetime_key)
    """)

def main():
    sp = spark()
    ensure_table(sp)

    # use last available hour in dim_datetime_enhanced, predict next hour
    dt = sp.table("lake.gold.dim_datetime_enhanced")
    max_key = dt.agg(F.max("datetime_key")).first()[0]
    next_key = int(max_key) + 1  # naive; ok for demo since dim is built incrementally by Phase 4

    # Build feature frame for all zones (use last known avg_speed/tip_pct per zone as fallback)
    hist = sp.table("lake.gold.fact_hourly_demand")
    last_stats = (hist.groupBy("zone_key")
                      .agg(F.avg("avg_speed").alias("avg_speed"),
                           F.avg("avg_tip_pct").alias("avg_tip_pct")))

    has_event = dt.filter(F.col("datetime_key")==next_key).select("datetime_key","has_major_event").first()
    has_event_val = bool(has_event.has_major_event) if has_event else False

    # Construct input
    zones = hist.select("zone_key").distinct()
    features = (zones
        .join(last_stats, "zone_key", "left")
        .withColumn("pickup_datetime_key", F.lit(next_key))
        .withColumn("hour", F.lit(int(str(next_key)[-2:])))
        .withColumn("has_major_event", F.lit(has_event_val))
        .fillna({"avg_speed":0.0, "avg_tip_pct":0.0})
    )

    model = PipelineModel.load(MODEL_URI)
    preds = model.transform(features).select(
        "pickup_datetime_key","zone_key", F.col("prediction").alias("predicted_trips")
    ).withColumn("model_uri", F.lit(MODEL_URI)).withColumn("created_ts", F.current_timestamp())

    preds.createOrReplaceTempView("preds_stage")

    sp.sql("""
        MERGE INTO lake.gold.pred_hourly_demand t
        USING preds_stage s
        ON t.pickup_datetime_key = s.pickup_datetime_key AND t.zone_key = s.zone_key
        WHEN MATCHED THEN UPDATE SET
          predicted_trips = s.predicted_trips,
          model_uri = s.model_uri,
          created_ts = s.created_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("[INFER] Upserted predictions for next hour.")
    sp.stop()

if __name__ == "__main__":
    main()
