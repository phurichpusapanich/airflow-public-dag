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
    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Niko",
            "last_name": "Koni"
        }

    @task()
    def get_age():
        return 29

    @task()
    def greet(name: dict, age: int):
        print(f"{name.get('first_name')} {name.get('last_name')} - {age}")

    # Compute first 2 tasks
    name_dict = get_name()
    age = get_age()
    # use the result in the 3rd task to create a DAG
    greet(name=name_dict, age=age)

# Initialise
greet_dag = hello_world_etl()