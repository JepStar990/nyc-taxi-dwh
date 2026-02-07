# Trains a global Gradient-Boosted Trees regressor to predict next-hour TRIPS by zone.
# Reads lake.gold.fact_hourly_demand as training data; writes model to s3a://nyc-tlc/models/forecast_gbt/
import os
from pyspark.sql import SparkSession, functions as F, types as T, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

MODEL_URI = os.getenv("FORECAST_MODEL_URI", "s3a://nyc-tlc/models/forecast_gbt")

def spark():
    return (
        SparkSession.builder.appName("train_hourly_forecast")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type","hive")
        .config("spark.sql.catalog.lake.uri","thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir","s3a://nyc-tlc/warehouse")
        .getOrCreate()
    )

def main():
    sp = spark()
    df = sp.table("lake.gold.fact_hourly_demand") \
           .select("pickup_datetime_key","zone_key","trips","avg_speed","avg_tip_pct","has_major_event")

    # Derive time features from key
    df = (df
        .withColumn("dt_str", F.col("pickup_datetime_key").cast("string"))
        .withColumn("year", F.substring("dt_str",1,4).cast("int"))
        .withColumn("month", F.substring("dt_str",5,2).cast("int"))
        .withColumn("day", F.substring("dt_str",7,2).cast("int"))
        .withColumn("hour", F.substring("dt_str",9,2).cast("int"))
        .drop("dt_str"))

    # Label: next hour's trips per (zone, hour)
    w = Window.partitionBy("zone_key").orderBy("pickup_datetime_key")
    df = df.withColumn("label_trips_next", F.lead("trips").over(w))
    df = df.dropna(subset=["label_trips_next"])

    cat_cols = ["zone_key"]
    num_cols = ["hour","avg_speed","avg_tip_pct","has_major_event"]
    stages = []

    # Categorical encoding
    for c in cat_cols:
        idx = StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        oh  = OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_oh")
        stages += [idx, oh]

    features = [f"{c}_oh" for c in cat_cols] + num_cols
    assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="keep")
    stages.append(assembler)

    gbt = GBTRegressor(featuresCol="features", labelCol="label_trips_next", maxDepth=6, maxIter=60, stepSize=0.1)
    stages.append(gbt)

    pipeline = Pipeline(stages=stages)
    train, test = df.randomSplit([0.85, 0.15], seed=42)
    model = pipeline.fit(train)
    preds = model.transform(test)

    rmse = RegressionEvaluator(predictionCol="prediction", labelCol="label_trips_next", metricName="rmse").evaluate(preds)
    mae  = RegressionEvaluator(predictionCol="prediction", labelCol="label_trips_next", metricName="mae").evaluate(preds)
    print(f"[TRAIN] RMSE={rmse:.3f} MAE={mae:.3f}")

    # Save/overwrite model
    model.write().overwrite().save(MODEL_URI)
    print(f"[TRAIN] Model saved to {MODEL_URI}")
    sp.stop()

if __name__ == "__main__":
    main()
