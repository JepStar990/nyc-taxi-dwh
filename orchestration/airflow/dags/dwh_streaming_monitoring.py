# orchestration/airflow/dags/dwh_streaming_monitoring.py
from datetime import datetime, timedelta
import subprocess, json
from airflow import DAG
from airflow.operators.python import PythonOperator

BROKER = "kafka:9092"
TOPICS = ["yellow_trips","green_trips","hvfhs_trips"]  # align with your simulator
MAX_AGE_MINUTES = 15

default_args = {
    "owner": "data-eng",
    "retries": 0
}

def check_kafka():
    # List topics
    cmd = ["/opt/kafka/bin/kafka-topics.sh","--bootstrap-server",BROKER,"--list"]
    # Airflow image doesn’t ship Kafka CLI; use container 'kafka' via bash (docker compose network)
    # Instead, just attempt TCP connect (nc) and rely on app-level metrics or logs.
    # We’ll keep a simple TCP check:
    import socket
    host, port = BROKER.split(":")[0], int(BROKER.split(":")[1])
    with socket.create_connection((host, port), timeout=5) as s:
        pass
    print("Kafka broker reachable.")

def check_topic_freshness():
    """
    Lightweight freshness check by reading the stream Bronze checkpoint files
    (if you enable streaming job later), or simply log that topics are configured.
    Placeholder as the streaming job is in Phase 6.
    """
    print(f"Configured topics: {TOPICS}; freshness monitoring will be fully wired in Phase 6.")

with DAG(
    dag_id="dwh_streaming_monitoring",
    default_args=default_args,
    start_date=datetime(2025,1,1),
    schedule_interval="*/10 * * * *",  # every 10 minutes
    catchup=False,
    tags=["production","monitoring"]
) as dag:

    broker = PythonOperator(task_id="broker_reachable", python_callable=check_kafka)
    freshness = PythonOperator(task_id="topic_freshness", python_callable=check_topic_freshness)

    broker >> freshness
