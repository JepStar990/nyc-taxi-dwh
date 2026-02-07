-- sql/ddl/star_schema.sql

-- Ensure namespace
CREATE NAMESPACE IF NOT EXISTS lake.gold;

-- ==============
-- Dimensions
-- ==============

-- Location SCD2
CREATE TABLE IF NOT EXISTS lake.gold.dim_location_scd2 (
  location_id INT,
  zone STRING,
  borough STRING,
  service_zone STRING,
  zone_key INT,              -- surrogate (same as location_id for current version)
  borough_key INT,           -- surrogate for borough
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN,
  version INT
)
USING iceberg
PARTITIONED BY (location_id);

-- Datetime enhanced (hour grain)
CREATE TABLE IF NOT EXISTS lake.gold.dim_datetime_enhanced (
  datetime_key INT,          -- yyyymmddHH
  dt TIMESTAMP,
  date_key INT,              -- yyyymmdd
  year INT,
  month INT,
  day INT,
  hour INT,
  day_of_week INT,           -- 1=Mon ... 7=Sun
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  has_major_event BOOLEAN
)
USING iceberg
PARTITIONED BY (date_key);

-- Vendor
CREATE TABLE IF NOT EXISTS lake.gold.dim_vendor (
  vendor_key INT,
  vendor_id INT,
  vendor_name STRING
)
USING iceberg;

-- Payment type
CREATE TABLE IF NOT EXISTS lake.gold.dim_payment_type (
  payment_type_key INT,
  payment_type_id INT,
  payment_type_desc STRING
)
USING iceberg;

-- Rate code
CREATE TABLE IF NOT EXISTS lake.gold.dim_rate_code (
  rate_code_key INT,
  rate_code_id INT,
  rate_code_desc STRING
)
USING iceberg;

-- Trip type
CREATE TABLE IF NOT EXISTS lake.gold.dim_trip_type (
  trip_type_key INT,
  trip_type_id INT,
  trip_type_desc STRING
)
USING iceberg;

-- ==============
-- Core Fact
-- ==============

CREATE TABLE IF NOT EXISTS lake.gold.fact_trip_detailed (
  trip_id STRING,                           -- deterministic hash id
  -- time dims
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  request_datetime TIMESTAMP,
  pickup_datetime_key INT,
  dropoff_datetime_key INT,
  request_datetime_key INT,
  -- location dims
  pickup_location_key INT,
  dropoff_location_key INT,
  pickup_borough_key INT,
  dropoff_borough_key INT,
  -- business dims
  vendor_key INT,
  payment_type_key INT,
  rate_code_key INT,
  trip_type_key INT,
  -- measures
  passenger_count INT,
  trip_distance DOUBLE,
  fare_amount DOUBLE,
  extra_amount DOUBLE,
  mta_tax_amount DOUBLE,
  improvement_surcharge_amount DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  congestion_surcharge_amount DOUBLE,
  airport_fee_amount DOUBLE,
  ehail_fee_amount DOUBLE,
  cbd_congestion_fee DOUBLE,
  total_amount DOUBLE,
  -- calculated
  trip_duration_seconds INT,
  trip_speed_mph DOUBLE,
  tip_percentage DOUBLE,
  fare_per_mile DOUBLE,
  is_shared_trip BOOLEAN,
  is_airport_trip BOOLEAN,
  is_peak_hour BOOLEAN,
  -- quality
  data_quality_score INT,
  is_suspicious BOOLEAN,
  -- audit
  service_type STRING,
  trip_date DATE,
  created_ts TIMESTAMP,
  modified_ts TIMESTAMP,
  source_system STRING
)
USING iceberg
PARTITIONED BY (trip_date);

-- Optimize default sort order (Iceberg v2 write-order)
ALTER TABLE lake.gold.fact_trip_detailed
  WRITE ORDERED BY (trip_date, pickup_datetime, pickup_location_key, dropoff_location_key);

-- ==============
-- Aggregates
-- ==============

CREATE TABLE IF NOT EXISTS lake.gold.fact_daily_zone_summary (
  trip_date DATE,
  zone_key INT,
  trips BIGINT,
  revenue DOUBLE,
  avg_miles DOUBLE,
  avg_minutes DOUBLE,
  suspicious_trips BIGINT
)
USING iceberg
PARTITIONED BY (trip_date);

CREATE TABLE IF NOT EXISTS lake.gold.fact_hourly_demand (
  pickup_datetime_key INT,
  zone_key INT,
  trips BIGINT,
  avg_speed DOUBLE,
  avg_tip_pct DOUBLE,
  weather_desc STRING,     -- optional join from external
  has_major_event BOOLEAN
)
USING iceberg
PARTITIONED BY (pickup_datetime_key);

CREATE TABLE IF NOT EXISTS lake.gold.fact_revenue_decomposition (
  trip_date DATE,
  service_type STRING,
  base_fare DOUBLE,
  extras DOUBLE,
  mta_tax DOUBLE,
  improvement_surcharge DOUBLE,
  tips DOUBLE,
  tolls DOUBLE,
  congestion DOUBLE,
  airport_fees DOUBLE,
  ehail DOUBLE,
  cbd_fee DOUBLE,
  total DOUBLE
)
USING iceberg
PARTITIONED BY (trip_date);

-- Retention & compaction hints (runtime tuning; honored by maintenance jobs)
ALTER TABLE lake.gold.fact_trip_detailed SET TBLPROPERTIES (
  'write.target-file-size-bytes'='268435456',   -- 256MB
  'commit.manifest.min-count-to-merge'='10'
);
