import datetime
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_function():
    print('Hello, this is the first task of the DAG')
    time.sleep(5)

def last_function():
    print('DAG run is done.')

def sleeping_function():
    print("Sleeping for 5 seconds")
    time.sleep(5)

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 4, 13),
    'catchup': False
}

with DAG(dag_id="local_executor_demo",
         default_args=default_args,
         schedule_interval="@hourly") as dag:

    task1 = PythonOperator(
        task_id="hello_1",
        python_callable=hello_function
    )

    task2_1 = PythonOperator(
        task_id="sleepy_1",
        python_callable=sleeping_function
    )

    task2_2 = PythonOperator(
        task_id="sleepy_2",
        python_callable=sleeping_function
    )

    task3 = PythonOperator(
        task_id="bye_function",
        python_callable=last_function
    )

    task1 >> [task2_1, task2_2] >> task3
