from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import logging
logging.basicConfig(level=logging.DEBUG)

default_args = {
    'owner': 'Niche',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='bash_operator_sample_v2',
    default_args=default_args,
    description='This is a sample version of bash operator',
    start_date=datetime(2024, 1, 4, 15),
    schedule_interval='@daily'

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo Hey, I am task 2!"
    )

    task3 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task1 >> task2 >> task3
