from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Niche',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='bash_operator_sample',
    default_args=default_args,
    description='This is a sample version of bash operator',
    start_date=datetime(2024, 1, 4, 15),
    schedule_interval='@daily'

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo 'hello world!'"
    )