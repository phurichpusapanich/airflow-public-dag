from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Niche',
}


def get_name():
    return "Niko"


def greet(age, ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello, name is {name} and I am {age} years old")


with DAG(
    default_args=default_args,
    dag_id='python_operator_dag_v1',
    description='Sample python operator dag',
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    schedule_interval='@daily'
) as dag:

    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={'age': 29}
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name,
    )

    task2 >> task1
