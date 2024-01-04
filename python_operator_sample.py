from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Niche',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def greet():
    print("HELLO EXAMPLE!!!")


with DAG(
    default_args=default_args,
    dag_id='python_operator_dag_v1',
    description='Sample python operator dag',
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    schedule_interval='@daily'
) as dag:

    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet
    )

    task1
