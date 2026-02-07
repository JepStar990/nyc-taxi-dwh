-- sql/ddl/semantic_views.sql

CREATE OR REPLACE VIEW lake.gold.v_revenue_daily AS
SELECT
  trip_date,
  service_type,
  base_fare + extras + mta_tax + improvement_surcharge + tips + tolls + congestion + airport_fees + ehail + cbd_fee AS total_revenue,
  base_fare, extras, mta_tax, improvement_surcharge, tips, tolls, congestion, airport_fees, ehail, cbd_fee
FROM lake.gold.fact_revenue_decomposition;

CREATE OR REPLACE VIEW lake.gold.v_hourly_demand_features AS
SELECT
  pickup_datetime_key,
  zone_key,
  trips,
  avg_speed,
  avg_tip_pct,
  has_major_event
FROM lake.gold.fact_hourly_demand;
