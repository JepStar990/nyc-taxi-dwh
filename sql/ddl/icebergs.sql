-- Create the Iceberg namespaces (databases) once
USE CATALOG lake;
CREATE NAMESPACE IF NOT EXISTS lake.raw;
CREATE NAMESPACE IF NOT EXISTS lake.bronze;
CREATE NAMESPACE IF NOT EXISTS lake.silver;
CREATE NAMESPACE IF NOT EXISTS lake.gold;
