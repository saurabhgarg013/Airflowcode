#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# initializing the default arguments
default_args = {
		'owner': 'Ranga',
		'start_date': datetime(2024, 4, 13),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}


# Instantiate a DAG object
hello_world_dag = DAG('hello_world_dag',
		default_args=default_args,
		description='Hello World DAG',
		schedule_interval='* * * * *', 
		catchup=False,
		tags=['example, helloworld']
)


#By default, Airflow will run tasks for all past intervals up to the current time. 
#This behavior can be disabled by setting the catchup parameter of a DAG to false


#DAG will be triggered to run every minute, or in other words, 
#it will attempt to execute every minute of every day.
# python callable function
def print_hello():
		return 'Hello World!'
		

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(task_id='hello_world_task', 
                                  python_callable=print_hello, 
                                  dag=hello_world_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=hello_world_dag)		

# Set the order of execution of tasks. 
start_task >> hello_world_task >> end_task