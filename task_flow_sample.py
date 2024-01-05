from airflow.decorators import dag, task

from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'Niche'
}


@dag(dag_id='dag_with_taskflow_api',
     default_args=default_args,
     start_date=pendulum.datetime(2024, 1, 5, tz="UTC"),
     schedule_interval='@daily')
def hello_world_etl():
    # Declare tasks
    @task()
    def get_name():
        return "Niko"

    @task()
    def get_age():
        return 29

    @task()
    def greet(name, age):
        print(f"{name} - {age}")

    # Compute first 2 tasks
    name = get_name()
    age = get_age()
    # use the result in the 3rd task to create a DAG
    greet(name=name, age=age)

# Initialise
greet_dag = hello_world_etl()