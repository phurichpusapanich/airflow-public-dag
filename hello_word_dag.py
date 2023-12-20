from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello World")

with DAG('hello_world_dag',
         start_date=datetime(2023, 1, 1),
         max_active_runs=3,
         schedule_interval=None,  # None means this DAG will only be run manually
         catchup=False) as dag:

    print_hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

print_hello_task