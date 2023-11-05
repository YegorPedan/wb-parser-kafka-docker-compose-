from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def run_consumer():

dag = DAG(
    "kafka_consumer_dag",
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

consumer_task = PythonOperator(
    task_id="run_consumer",
    python_callable=run_consumer,
    dag=dag,
)
