# File: dags/spotify_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from your Spotify DAG!")

with DAG(
    dag_id="spotify_insights",
    start_date=datetime(2025, 5, 20),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )
