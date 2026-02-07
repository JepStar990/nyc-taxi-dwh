-- Roll back Gold facts to a previous snapshot
-- Usage:
-- 1) Find snapshot_id: SELECT * FROM lake.gold.fact_trip_detailed$snapshots ORDER BY committed_at DESC;
-- 2) SET a session var in your client then run the time travel SELECT or CREATE TABLE AS SELECT.

-- Example read at snapshot:
-- SELECT * FROM lake.gold.fact_trip_detailed FOR VERSION AS OF <snapshot_id> LIMIT 10;

-- To restore content (create a copy at old snapshot):
-- CREATE TABLE lake.gold.fact_trip_detailed_rollback AS
--   SELECT * FROM lake.gold.fact_trip_detailed FOR VERSION AS OF <snapshot_id>;

-- Or to "revert" by overwriting current table with snapshot (Spark SQL approach):
-- WARNING: overwriting is destructive; make a copy if unsure.
