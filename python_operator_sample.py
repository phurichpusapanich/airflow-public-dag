from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Niche',
}


def get_name(ti):
    ti.xcom_push(key='first_name', value='Niko')
    ti.xcom_push(key='last_name', value='Koni')


def get_age(ti):
    ti.xcom_push(key='age', value=29)


def greet(ti):
    name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello, name is {name} {last_name} and I am {age} years old")


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

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )

    [task3, task2] >> task1
