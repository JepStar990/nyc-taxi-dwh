# NYC Taxi Data Warehouse

> **Status**: ✅ Production-ready  
> **Stack**: Spark 3.5 • Apache Iceberg • Trino • MinIO (S3) • Hive Metastore • Kafka • Airflow • Superset • Great Expectations (hooks) • OpenLineage/Marquez • Prometheus/Grafana • Feast (features) • Redis (online store) • HashiCorp Vault (secrets)  
> **Patterns**: Medallion (RAW → Bronze → Silver → Gold), Lambda (batch + speed), Star Schema, Incremental MERGE, Data Contracts, SCD2

***

## Table of Contents

1.  \#what-this-project-delivers
2.  \#architecture
3.  \#repository-structure
4.  \#prerequisites
5.  \#quick-start-local-docker
6.  \#data-model--contracts
7.  \#pipelines--phases
8.  \#streaming-speed-layer
9.  \#data-quality--contracts
10. \#governance-rbac--views
11. \#gold-star-schema--aggregates
12. \#bi--superset
13. \#monitoring--lineage
14. \#cicd--environment-promotion
15. \#maintenance--cost-optimization
16. \#machine-learning-features-training-inference
17. \#data-sharing-snapshots--presigned-links
18. \#secrets-management-with-vault
19. \#operational-runbooks
20. \#environment-variables
21. \#troubleshooting
22. \#roadmap-optional-enhancements
23. \#license

***

## What this project delivers

*   **End-to-end production lakehouse** for NYC Taxi data with **batch** and **streaming** ingestion.
*   **Medallion layers** with **data contracts**, **DQ rules**, **anomaly flags**, and **incremental Gold**.
*   **Star schema** for core analytics + **aggregates** for BI performance.
*   **Dashboards** on Superset connected to Trino/Iceberg.
*   **Airflow** orchestration: daily medallion runs, backfills, streaming monitoring, weekly Iceberg maintenance.
*   **OpenLineage → Marquez** lineage + Prometheus/Grafana metrics.
*   **ML training & inference** for **hour-ahead demand by zone**; **Feast** features with **Redis** online store.
*   **Data sharing** via Parquet snapshots + MinIO **presigned URLs**.
*   **Secrets** via **Vault** (e.g., Slack webhooks) — no cleartext in code.

***

## Architecture

**Compute**: Spark for batch & streaming; Trino for interactive SQL/BI  
**Storage**: MinIO (S3-compatible) + Apache Iceberg tables + Hive Metastore  
**Serving**: Trino → Superset (dashboards)  
**Orchestrator**: Airflow  
**Streaming**: Kafka → Spark Structured Streaming → Bronze  
**Governance**: Data contracts (era-based), RBAC (Trino), governed views  
**Observability**: Prometheus/Grafana, OpenLineage/Marquez, DQ results

    [Producers/Files] -> RAW (S3/MinIO, Iceberg) -> Bronze (standardized)
                                                 -> Silver (validated + enriched)
                                                 -> Gold (star schema + aggregates)
                                                 -> BI (Superset/Trino)
                                                 -> ML (Features/Model/Predictions)

***

## Repository structure

> Snapshot (key folders only)

nyc-taxi-dwh/
├── LICENSE
├── Makefile
├── README.md
├── bi
│   └── superset
│       ├── dashboards
│       │   ├── core_trip_analysis.json
│       │   ├── driver_shift_analysis.json
│       │   ├── revenue_decomposition.json
│       │   └── zone_pair_analysis.json
│       ├── superset_bootstrap.sh
│       └── superset_register_datasets.py
├── configs
│   ├── airflow
│   │   └── airflow.cfg
│   ├── dq
│   │   ├── bronze_rules.yml
│   │   ├── gold_rules.yml
│   │   └── silver_rules.yml
│   ├── feast
│   │   └── feature_store.yaml
│   ├── great_expectations
│   │   └── great_expectations.yml
│   ├── iceberg
│   │   ├── spark-catalog.conf
│   │   ├── table-defaults.yaml
│   │   └── trino-catalog.properties
│   ├── spark
│   │   ├── log4j2.properties
│   │   └── spark-defaults.conf
│   └── trino
│       ├── config.properties
│       └── jvm.config
├── contracts
│   ├── dictionaries
│   │   ├── data_dictionary_trip_records_fhv.pdf
│   │   ├── data_dictionary_trip_records_green.pdf
│   │   ├── data_dictionary_trip_records_hvfhs.pdf
│   │   └── data_dictionary_trip_records_yellow.pdf
│   └── schemas
│       ├── fhv
│       │   └── v_all.avsc
│       ├── green
│       │   ├── v2015-2018.avsc
│       │   ├── v2019-2024.avsc
│       │   └── v2025+.avsc
│       ├── hvfhs
│       │   └── v2019+.avsc
│       └── yellow
│           ├── v2015-2018.avsc
│           ├── v2019-2024.avsc
│           └── v2025+.avsc
├── data
│   ├── external
│   │   ├── events_2019-01.csv
│   │   └── weather_hourly_2019-01.csv
│   └── samples
│       ├── fhv
│       │   └── 2018-10.parquet
│       ├── green
│       │   └── 2019-01.parquet
│       ├── hvfhs
│       │   └── 2019-03.parquet
│       ├── taxi_zone_lookup.csv
│       └── yellow
│           └── 2019-01.parquet
├── dbt
│   ├── dbt_project.yml
│   ├── macros
│   │   ├── dq_helpers.sql
│   │   ├── scd2.sql
│   │   └── zorder.sql
│   ├── models
│   │   ├── bronze
│   │   │   ├── stg_fhv.sql
│   │   │   ├── stg_green.sql
│   │   │   ├── stg_hvfhs.sql
│   │   │   └── stg_yellow.sql
│   │   ├── docs
│   │   │   └── glossary.md
│   │   ├── exposures.yml
│   │   ├── gold
│   │   │   ├── fact_daily_zone_summary.sql
│   │   │   ├── fact_driver_shift_analysis.sql
│   │   │   ├── fact_hourly_demand.sql
│   │   │   └── fact_revenue_decomposition.sql
│   │   └── silver
│   │       ├── dim_datetime_enhanced.sql
│   │       ├── dim_location_scd2.sql
│   │       ├── dim_vendor.sql
│   │       ├── fct_trip_detailed.sql
│   │       └── tests.yml
│   └── profiles_template.yml
├── docker-compose.yml
├── infra
│   ├── docker
│   │   ├── docker-compose.yml
│   │   └── images
│   │       ├── airflow
│   │       │   └── Dockerfile
│   │       ├── spark
│   │       │   └── Dockerfile
│   │       ├── superset
│   │       │   └── Dockerfile
│   │       └── trino
│   │           └── Dockerfile
│   ├── hive
│   │   └── postgres.jar
│   ├── k8s
│   │   ├── charts
│   │   ├── helmfile.yaml
│   │   └── values
│   │       ├── kafka.yaml
│   │       ├── marquez.yaml
│   │       ├── minio.yaml
│   │       ├── prometheus-grafana.yaml
│   │       ├── spark-operator.yaml
│   │       ├── superset.yaml
│   │       └── trino.yaml
│   └── terraform
│       └── README.md
├── justfile
├── ml
│   ├── features
│   │   ├── demand_features.py
│   │   └── feast_repo
│   │       ├── entities.py
│   │       ├── feature_store.yaml
│   │       ├── feature_views.py
│   │       └── materialize.sh
│   ├── notebooks
│   │   ├── 01_demand_forecast.ipynb
│   │   ├── 02_anomaly_detection.ipynb
│   │   └── 03_recommendations.ipynb
│   ├── pipelines
│   │   ├── batch_inference_forecast_spark.py
│   │   └── train_forecast_spark.py
│   └── serving
│       └── fastapi_features_service
│           ├── Dockerfile
│           └── app.py
├── monitoring
│   ├── grafana
│   │   └── dashboards
│   │       ├── cost_monitoring.json
│   │       ├── dq_sla.json
│   │       └── pipeline_health.json
│   ├── openlineage
│   │   └── marquez_bootstrap.sh
│   └── prometheus
│       └── alerts.yml
├── orchestration
│   ├── airflow
│   │   └── dags
│   │       ├── data_sharing_exports.py
│   │       ├── dwh_backfill_monthly.py
│   │       ├── dwh_maintenance_iceberg.py
│   │       ├── dwh_medallion_daily.py
│   │       ├── dwh_streaming_monitoring.py
│   │       ├── lib
│   │       │   └── secrets.py
│   │       ├── ml_training_and_materialization.py
│   │       └── vault_bootstrap.py
│   └── prefect
│       └── flows
│           └── medallion_flow.py
├── pipelines
│   ├── __init__.py
│   ├── ingestion
│   │   ├── __init__.py
│   │   ├── raw_to_bronze_batch.py
│   │   ├── seed_raw_from_samples.py
│   │   ├── stream_simulator
│   │   │   ├── produce_green.py
│   │   │   ├── produce_hvfhs.py
│   │   │   └── produce_yellow.py
│   │   └── stream_to_bronze_spark.py
│   ├── quality
│   │   ├── dq_bronze.py
│   │   ├── dq_silver.py
│   │   ├── ml_anomaly_detection.py
│   │   └── storage_healthcheck.py
│   ├── sharing
│   │   └── export_gold_shares.py
│   ├── transform
│   │   ├── bronze_to_silver.py
│   │   ├── incremental
│   │   │   ├── cdc_dimensions.py
│   │   │   └── partial_aggregate_refresh.py
│   │   ├── maintenance_iceberg.py
│   │   └── silver_to_gold.py
│   └── utils
│       ├── __init__.py
│       ├── business_rules.py
│       ├── contracts.py
│       ├── dq.py
│       ├── io.py
│       ├── schemas.py
│       └── spark_session.py
├── scripts
│   ├── bootstrap_docker.sh
│   ├── bootstrap_minikube.sh
│   ├── create_kafka_topics.sh
│   ├── iceberg_snapshot_rollback.sh
│   ├── load_sample_data.sh
│   ├── maintenance_now.sh
│   ├── minio_presign.sh
│   ├── port_forward.sh
│   ├── run_stream_ingest.sh
│   ├── spark_submit_local.sh
│   ├── trino_cli.sh
│   └── vault_bootstrap.sh
└── sql
    ├── ddl
    │   ├── grants.sql
    │   ├── icebergs.sql
    │   ├── rollback.sql
    │   ├── security_views.sql
    │   ├── semantic_views.sql
    │   └── star_schema.sql
    ├── gold_views
    │   ├── mv_daily_zone_summary.sql
    │   ├── mv_revenue_trends_rolling30.sql
    │   └── mv_top_corridors.sql
    └── queries
        └── ops_kpis.sql

76 directories, 151 files

***

## Prerequisites

*   Docker & Docker Compose
*   Python 3.10+ (for local producer scripts)
*   \~8–12 GB RAM for the full stack
*   Optional: Trino CLI, jq, make/just

***

## Quick start (local, Docker)

1.  **Start services & seed RAW**

```bash
docker compose up -d
./scripts/bootstrap_docker.sh
```

2.  **Bronze → Silver → Gold** (manual runs)

```bash
./scripts/spark_submit_local.sh pipelines/ingestion/raw_to_bronze_batch.py
./scripts/spark_submit_local.sh pipelines/transform/bronze_to_silver.py
./scripts/spark_submit_local.sh pipelines/transform/silver_to_gold.py --days 7
```

3.  **Superset**

```bash
docker compose build superset && docker compose up -d superset
docker compose exec -T superset bash -lc "/workspace/bi/superset/superset_bootstrap.sh"
# open http://localhost:8089
```

4.  **Airflow DAGs**  
    Open <http://localhost:8088> → unpause:

*   `dwh_medallion_daily` (daily)
*   `dwh_maintenance_iceberg` (weekly)
*   `ml_training_and_materialization` (daily ML)
*   `data_sharing_exports` (daily export)

5.  **Streaming (optional)**

```bash
./scripts/create_kafka_topics.sh
./scripts/run_stream_ingest.sh --continuous
```

***

## Data model & contracts

*   **RAW**: source-accurate Iceberg tables per dataset (`lake.raw.yellow|green|hvfhs|fhv`).
*   **Bronze**: **unified** standardized table `lake.bronze.trip_records` (one-wide schema).
*   **Silver**: `lake.silver.trip_enriched` adds **duration, speed, tip%**, **flags**, **DQ score**.
*   **Gold**: Facts & dims (see below).

**Contracts (era-aware)**  
`pipelines/utils/contracts.py` validates that RAW tables match expected columns per **service** and **era** (e.g., Yellow/Green 2025+ allowing `cbd_congestion_fee`). The **daily Airflow DAG** runs a **contracts gate** (`--validate-only`) **before** transformations.

Violations are logged to `lake.raw._contract_results` and the DAG fails fast.

***

## Pipelines & phases

*   **Phase 1**: Infra + RAW seeding (`scripts/bootstrap_docker.sh`, `pipelines/ingestion/seed_raw_from_samples.py`)
*   **Phase 2**: Bronze standardization (`pipelines/ingestion/raw_to_bronze_batch.py`)
*   **Phase 3**: Silver cleanse/enrich (`pipelines/transform/bronze_to_silver.py` + `utils/business_rules.py` & `utils/dq.py`)
*   **Phase 4**: Gold star schema & aggregates (`pipelines/transform/silver_to_gold.py`, `sql/ddl/star_schema.sql`)
*   **Phase 5**: Orchestration, lineage, DQ alerts (Airflow DAGs)
*   **Phase 6**: Streaming from Kafka to Bronze (`pipelines/ingestion/stream_to_bronze_spark.py`)
*   **Phase 7**: Governance (RBAC grants, governed views, dbt exposures)
*   **Phase 8**: CI/CD (GitHub Workflows), promotion & rollbacks
*   **Phase 9**: Iceberg maintenance & storage metrics
*   **Phase 10**: Superset bootstrap, datasets, dashboards

***

## Streaming (speed layer)

*   Kafka topics: `yellow_trips`, `green_trips`, `hvfhs_trips`
*   **Spark Structured Streaming** reads JSON → canonicalizes → **dedup (watermark)** → append to Bronze with exactly-once micro-batch semantics via checkpoints (`s3a://nyc-tlc/checkpoints/...`).
*   Start:

```bash
./scripts/create_kafka_topics.sh
./scripts/run_stream_ingest.sh --continuous
```

*   Produce messages using your simulator or Kafka console producer.

***

## Data quality & contracts

*   **Contracts gate** (RAW era checks) — blocks downstream work if shape is wrong.
*   **Silver-level validations**:
    *   Temporal (dropoff > pickup, 1 min ≤ duration ≤ 24h)
    *   Spatial (LocationID in 1..265)
    *   Financial (Σ components ≈ total within tolerance)
    *   Business (airport heuristics, zero-distance fares, speed bounds)
    *   Composite `data_quality_score` + `is_suspicious` flag
*   **DQ results** (`lake.silver._dq_results`) drive Grafana panels & Slack alerts (optional via Vault secret).

***

## Governance, RBAC & views

*   Trino roles: `data_engineer`, `data_scientist`, `data_analyst`, `business_user`
*   Grants in `sql/ddl/grants.sql` enforce **least privilege** (Business only on governed views).
*   Governed view (borough-only): `lake.gold.v_fact_trip_borough_only` (`sql/ddl/security_views.sql`)
*   dbt **exposures** (`dbt/models/exposures.yml`) + **glossary** (business terms).

***

## Gold star schema & aggregates

**Dimensions**

*   `dim_location_scd2` (zone history; seeded from `taxi_zone_lookup.csv`)
*   `dim_datetime_enhanced` (hour grain; event/holiday flags)
*   `dim_vendor`, `dim_payment_type`, `dim_rate_code`, `dim_trip_type`

**Fact**

*   `fact_trip_detailed` — **MERGE** upserts with deterministic `trip_id`, partitioned by `trip_date`, sorted by `(trip_date, pickup_datetime, pickup_location_key, dropoff_location_key)`

**Aggregates**

*   `fact_daily_zone_summary` — daily per zone
*   `fact_hourly_demand` — features for ML
*   `fact_revenue_decomposition` — fare component trends
*   (ML) `pred_hourly_demand` — hour-ahead predictions per zone

***

## BI / Superset

*   **Image** installs Trino SQLAlchemy dialect.
*   **Bootstrap** script:
    *   Creates admin user (configurable)
    *   Registers Trino DB connection (catalog: `lake`)
    *   Registers datasets for Gold tables/views
    *   Imports dashboards from `bi/superset/dashboards/*.json`

Run:

```bash
docker compose build superset && docker compose up -d superset
docker compose exec -T superset bash -lc "/workspace/bi/superset/superset_bootstrap.sh"
# http://localhost:8089
```

***

## Monitoring & lineage

*   **Prometheus/Grafana**: pipeline health, DQ pass rates, storage metrics
*   **Storage metrics** job writes to `lake.admin.storage_metrics` (files, sizes, small-file count).
*   **OpenLineage → Marquez**: Airflow/Spark job & dataset lineage.
*   Airflow DAGs:
    *   `dwh_medallion_daily` (daily medallion + DQ alert)
    *   `dwh_streaming_monitoring` (broker reachability + hooks)
    *   `dwh_maintenance_iceberg` (weekly maintenance)
    *   `ml_training_and_materialization`, `data_sharing_exports`

***

## CI/CD & environment promotion

*   **CI** (`.github/workflows/ci.yml`): lint (ruff/black), schema checks, import sanity, image build.
*   **CD** (`.github/workflows/deploy.yml`):
    *   `env=dev` → Compose up (smoke Trino)
    *   `env=staging` → Minikube + Helmfile apply
*   **Rollbacks**:
    *   Data: Iceberg time-travel helper (`scripts/iceberg_snapshot_rollback.sh`)
    *   Code: Redeploy prior image tag

***

## Maintenance & cost optimization

*   **Iceberg procedures** (weekly by DAG):
    *   `rewrite_data_files` (compaction + write order)
    *   `rewrite_manifests` (metadata reduction)
    *   `expire_snapshots` (retention: keep N & older-than)
*   Tunables via env (`ICEBERG_EXPIRE_DAYS`, etc.)
*   **Storage metrics** table for Grafana.

Manual:

```bash
./scripts/maintenance_now.sh
```

***

## Machine Learning (features, training, inference)

**Training** (`ml/pipelines/train_forecast_spark.py`):

*   Trains GBT to predict **next-hour `trips`** by zone using `fact_hourly_demand`.
*   Saves model at `s3a://nyc-tlc/models/forecast_gbt`.

**Inference** (`ml/pipelines/batch_inference_forecast_spark.py`):

*   Generates **next-hour predictions** per zone → `lake.gold.pred_hourly_demand` (MERGE).

**Feast + Redis** (online features):

*   `configs/feast/feature_store.yaml` set to Redis online store.
*   Daily materialize in ML DAG.

**Serving** (FastAPI):

*   `ml/serving/fastapi_features_service` exposes `/features/demand` (reads Feast online features).
*   Extendable to a predictions API reading `pred_hourly_demand`.

Run ML:

```bash
./scripts/spark_submit_local.sh ml/pipelines/train_forecast_spark.py
./scripts/spark_submit_local.sh ml/pipelines/batch_inference_forecast_spark.py
```

Airflow:

*   `ml_training_and_materialization` (train → infer → feast materialize daily)

***

## Data sharing (snapshots + presigned links)

*   Export Gold as **Parquet snapshots**:
    *   `pipelines/sharing/export_gold_shares.py` → `s3a://nyc-tlc/shares/<table>/snapshot_ts=YYYYMMDDHHMM/`
*   Generate **presigned URLs** via MinIO client:
    *   DAG `data_sharing_exports` generates **24h** links
    *   Manual helper `scripts/minio_presign.sh <table>`

***

## Secrets management with Vault

*   Vault dev in Compose (port **8200**, token `myroot`).
*   `scripts/vault_bootstrap.sh` seeds demo secret (`secret/data/airflow/slack`).
*   Airflow reads secrets through HashiCorp provider (helper `orchestration/airflow/dags/lib/secrets.py`).
*   Daily DAG uses env `SLACK_WEBHOOK` or falls back to Vault (optional tiny patch in DAG shown in code comments).

***

## Operational runbooks

### Daily

*   Ensure `dwh_medallion_daily` succeeded; check Grafana **DQ pass rates** and **pipeline latency**.
*   Investigate any **contract violations** in `lake.raw._contract_results`.

### Weekly

*   Verify `dwh_maintenance_iceberg` reduced small-file counts in `lake.admin.storage_metrics`.
*   Spot-check dashboard **P95 query times** (<5s target).

### Backfill

*   Use `dwh_backfill_monthly` (full refresh MERGE-safe).
*   For streaming gaps, run `./scripts/run_stream_ingest.sh --available-now` to catch up and exit.

### Rollback

*   Data:
    1.  Inspect snapshots: `SELECT * FROM lake.gold.fact_trip_detailed$snapshots ORDER BY committed_at DESC;`
    2.  `./scripts/iceberg_snapshot_rollback.sh lake.gold.fact_trip_detailed <snapshot_id>`
*   Code:
    *   Redeploy previous image tag via CD workflow (`deploy.yml`) or change `.env` `IMAGE_TAG` for Compose.

***

## Environment variables

Common (Airflow & Spark jobs):

*   `AWS_ACCESS_KEY_ID=minio`, `AWS_SECRET_ACCESS_KEY=minio123`, `S3_ENDPOINT=http://minio:9000`
*   `OPENLINEAGE_URL=http://marquez:5000`, `OPENLINEAGE_NAMESPACE=nyc-taxi-dwh`
*   `ICEBERG_EXPIRE_DAYS`, `ICEBERG_RETAIN_LAST`, `ICEBERG_TARGET_FILE_MB`, `ICEBERG_MIN_INPUT_FILES`
*   ML: `FORECAST_MODEL_URI=s3a://nyc-tlc/models/forecast_gbt`
*   Feast: `configs/feast/feature_store.yaml` (Redis online store)
*   Superset: `SUPERSET_USER`, `SUPERSET_PASSWORD`, `TRINO_URI`, `TRINO_DB_NAME`
*   Vault: `VAULT_ADDR=http://vault:8200`, `VAULT_TOKEN=myroot`

***

## Troubleshooting

*   **Trino not reachable**: `docker compose logs trino`; verify hive-metastore is up and `trino-catalog.properties` is mounted.
*   **Spark S3 errors**: check `S3_ENDPOINT`, credentials, and that Spark has `hadoop-aws` + AWS SDK jars (already in images).
*   **Iceberg table not found**: ensure namespaces exist (`sql/ddl/icebergs.sql`) and jobs wrote tables into `lake.*`.
*   **Superset import errors**: make sure datasets are registered before importing dashboards (`superset_register_datasets.py`).
*   **Streaming not writing**: confirm Kafka connector jars exist in Spark image; topics exist; producer sends valid JSON; check streaming checkpoint path.
*   **DQ alerts not posting**: ensure Slack webhook in env or Vault secret path is correct.
*   **Vault**: if dev server restarted, re-run `scripts/vault_bootstrap.sh`.

***

## Roadmap (optional enhancements)

*   **MLflow** experiment tracking & model registry
*   **Row-level security** rules in Superset (beyond governed views)
*   **Delta Sharing / Iceberg REST** external sharing
*   **Autoscaling Spark-on-K8s** with Spark Operator, cluster autoscaler
*   **Distributed MinIO** for HA

***

## License

Distributed under the **MIT License** (see `LICENSE`).

***

### Handy commands (cheat sheet)

```bash
# Start or restart core services
docker compose up -d

# Run Bronze → Silver → Gold
./scripts/spark_submit_local.sh pipelines/ingestion/raw_to_bronze_batch.py
./scripts/spark_submit_local.sh pipelines/transform/bronze_to_silver.py
./scripts/spark_submit_local.sh pipelines/transform/silver_to_gold.py --days 7

# Streaming
./scripts/create_kafka_topics.sh
./scripts/run_stream_ingest.sh --continuous

# Maintenance & metrics
./scripts/maintenance_now.sh
docker compose exec -T spark /opt/spark/bin/spark-sql -e "SELECT * FROM lake.admin.storage_metrics ORDER BY run_ts DESC LIMIT 20;"

# Superset bootstrap
docker compose exec -T superset bash -lc "/workspace/bi/superset/superset_bootstrap.sh"

# Vault demo secrets
./scripts/vault_bootstrap.s
```
