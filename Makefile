.PHONY: up down ci run-silver run-gold stream topics dq

up:
\tdocker compose up -d

down:
\tdocker compose down -v

ci:
\truff check . && black --check .

run-silver:
\t./scripts/spark_submit_local.sh pipelines/transform/bronze_to_silver.py

run-gold:
\t./scripts/spark_submit_local.sh pipelines/transform/silver_to_gold.py --days 7

topics:
\t./scripts/create_kafka_topics.sh

stream:
\t./scripts/run_stream_ingest.sh --continuous

dq:
\tdocker compose exec -T spark /opt/spark/bin/spark-sql -e "SELECT * FROM lake.silver._dq_results ORDER BY run_ts DESC, rule_name"
