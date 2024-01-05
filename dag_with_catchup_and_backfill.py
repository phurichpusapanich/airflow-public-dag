from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import logging
import pendulum
logging.basicConfig(level=logging.DEBUG)

default_args = {
    'owner': 'Niche',
}

with DAG(
    dag_id='backfill_and_catch_up',
    default_args=default_args,
    description='Sample of backfill and catchup',
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    schedule_interval='@daily',
    catchup=True

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo "Hello World Task 1" && sleep 1'
    )

    task1