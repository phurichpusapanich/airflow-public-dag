from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import logging
import pendulum
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
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    schedule_interval='@daily'

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo "Hello World Task 1" && sleep 1'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo "Hello World Task 2" && sleep 1'
    )

    task3 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task1 >> task2 >> task3
