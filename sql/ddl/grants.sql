-- sql/ddl/grants.sql
-- Create roles
CREATE ROLE IF NOT EXISTS data_engineer;
CREATE ROLE IF NOT EXISTS data_scientist;
CREATE ROLE IF NOT EXISTS data_analyst;
CREATE ROLE IF NOT EXISTS business_user;

-- Grant USAGE on catalog & schemas
GRANT USAGE ON CATALOG lake TO data_engineer, data_scientist, data_analyst, business_user;
GRANT USAGE ON SCHEMA lake.gold   TO data_engineer, data_scientist, data_analyst, business_user;
GRANT USAGE ON SCHEMA lake.silver TO data_engineer, data_scientist, data_analyst;
GRANT USAGE ON SCHEMA lake.bronze TO data_engineer, data_scientist;
GRANT USAGE ON SCHEMA lake.raw    TO data_engineer;

-- Table-level privileges
-- Engineers: everything
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lake.raw    TO data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lake.bronze TO data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lake.silver TO data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lake.gold   TO data_engineer;

-- Scientists: read historical + features (gold & silver)
GRANT SELECT ON ALL TABLES IN SCHEMA lake.silver TO data_scientist;
GRANT SELECT ON ALL TABLES IN SCHEMA lake.gold   TO data_scientist;

-- Analysts: gold only
GRANT SELECT ON ALL TABLES IN SCHEMA lake.gold   TO data_analyst;

-- Business users: governed views only (see security_views.sql)
-- (Do NOT grant fact tables directly)
