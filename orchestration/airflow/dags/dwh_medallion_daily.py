# orchestration/airflow/dags/dwh_medallion_daily.py
from datetime import datetime, timedelta
import json, os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests

DEFAULT_DAYS = int(os.getenv("GOLD_INCREMENTAL_DAYS", "7"))
DQ_MIN_PASS_RATE = float(os.getenv("DQ_MIN_PASS_RATE", "0.95"))
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

spark_submit = "/opt/spark/bin/spark-submit"
spark_conf = [
    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf", "spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "--conf", "spark.sql.catalog.lake.type=hive",
    "--conf", "spark.sql.catalog.lake.uri=thrift://hive-metastore:9083",
    "--conf", "spark.sql.warehouse.dir=s3a://nyc-tlc/warehouse",
    "--conf", "spark.hadoop.fs.s3a.endpoint="+os.getenv("S3_ENDPOINT","http://minio:9000"),
    "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf", "spark.hadoop.fs.s3a.access.key="+os.getenv("AWS_ACCESS_KEY_ID","minio"),
    "--conf", "spark.hadoop.fs.s3a.secret.key="+os.getenv("AWS_SECRET_ACCESS_KEY","minio123"),
    "--conf", "spark.openlineage.transport.type=http",
    "--conf", "spark.openlineage.transport.url="+os.getenv("OPENLINEAGE_URL","http://marquez:5000"),
    "--conf", "spark.openlineage.namespace="+os.getenv("OPENLINEAGE_NAMESPACE","nyc-taxi-dwh"),
]

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=60)
}

with DAG(
    dag_id="dwh_medallion_daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["production","medallion","daily"]
) as dag:

    wait_services = BashOperator(
        task_id="wait_services",
        bash_command="""
        for i in {1..60}; do nc -z hive-metastore 9083 && break; sleep 2; done
        for i in {1..60}; do nc -z minio 9000 && break; sleep 2; done
        for i in {1..60}; do nc -z trino 8080 && break; sleep 2; done
        echo "Dependencies up"
        """
    )

    contracts_validate = BashOperator(
        task_id="contracts_validate",
        bash_command=" ".join([spark_submit, *spark_conf, "/workspace/pipelines/ingestion/raw_to_bronze_batch.py", "--validate-only"])
    )

    silver = BashOperator(
        task_id="silver_cleanse_enrich",
        bash_command=" ".join([spark_submit, *spark_conf, "/workspace/pipelines/transform/bronze_to_silver.py"])
    )

    gold = BashOperator(
        task_id="gold_star_schema_incremental",
        bash_command=" ".join([spark_submit, *spark_conf, "/workspace/pipelines/transform/silver_to_gold.py", "--days", str(DEFAULT_DAYS)])
    )

    def dq_check_and_alert():
        q = """
        WITH latest AS (SELECT max(run_ts) AS run_ts FROM lake.silver._dq_results)
        SELECT rule_name, pass_rate
        FROM lake.silver._dq_results, latest
        WHERE _dq_results.run_ts = latest.run_ts
        ORDER BY rule_name
        """
        trino_url = "http://trino:8080/v1/statement"
        headers = {"X-Trino-User":"airflow","Content-Type":"application/json"}
        r = requests.post(trino_url, headers=headers, data=json.dumps({"query":q}), timeout=60); r.raise_for_status()
        data = r.json()
        while "nextUri" in data:
            r = requests.get(data["nextUri"], timeout=60); r.raise_for_status(); data = r.json()
        rows = data.get("data", []) or []
        bad = [(rule, float(rate)) for rule, rate in rows if float(rate) < DQ_MIN_PASS_RATE]
        if not bad:
            print("All DQ rules above threshold."); return
        msg = "*DQ Alert* – rules below threshold (min pass rate {:.0%}):\n{}".format(
            DQ_MIN_PASS_RATE, "\n".join([f"- `{r}`: {p:.1%}" for r,p in bad])
        )
        print(msg)
        if SLACK_WEBHOOK:
            try: requests.post(SLACK_WEBHOOK, json={"text": msg}, timeout=10)
            except Exception as e: print(f"Slack post failed: {e}")

    dq_alerts = PythonOperator(task_id="dq_alerts", python_callable=dq_check_and_alert)

    wait_services >> contracts_validate >> silver >> gold >> dq_alerts
