-- sql/ddl/security_views.sql

CREATE OR REPLACE VIEW lake.gold.v_fact_trip_borough_only AS
SELECT
  f.trip_date,
  f.service_type,
  f.pickup_borough_key,
  f.dropoff_borough_key,
  f.passenger_count,
  f.trip_distance,
  f.total_amount,
  f.tip_amount,
  f.congestion_surcharge_amount,
  f.cbd_congestion_fee,
  f.is_airport_trip
FROM lake.gold.fact_trip_detailed f;

-- Privileges
GRANT SELECT ON lake.gold.v_fact_trip_borough_only TO data_analyst, business_user;
