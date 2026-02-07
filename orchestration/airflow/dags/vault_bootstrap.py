from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.secrets import get_vault_webhook

def print_secret():
    v = get_vault_webhook()
    print(f"Vault Slack webhook present: {'YES' if v else 'NO'}")

with DAG(
    dag_id="vault_bootstrap_demo",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["secrets","vault","demo"]
) as dag:
    show = PythonOperator(task_id="check_webhook", python_callable=print_secret)
