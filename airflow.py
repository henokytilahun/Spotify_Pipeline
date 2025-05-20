from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_history():
    df = pd.read_csv("/path/to/spotify_streaming_history.csv", parse_dates=["played_at","date"])
    engine = create_engine("postgresql+psycopg2://user:pass@host/db")
    df.to_sql("listening_history", engine, if_exists="append", index=False)

with DAG("spotify_history_load", start_date=datetime(2025,5,20), schedule_interval="@daily") as dag:
    task = PythonOperator(
        task_id="load_streaming_history",
        python_callable=load_history
    )
