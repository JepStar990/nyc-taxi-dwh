up:
\tdocker compose up -d

down:
\tdocker compose down -v

silver:
\t./scripts/spark_submit_local.sh pipelines/transform/bronze_to_silver.py

gold days="7":
\t./scripts/spark_submit_local.sh pipelines/transform/silver_to_gold.py --days {{days}}
