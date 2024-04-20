from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import pandas as pd

# Define the function to be executed by the PythonOperator
def process_data():
    # Create a dictionary with sample data
    data = {
        'sal': [1000, 2000, 3000, 4000, 5000],
        'ename': ['sam', 'alan', 'jack', 'rock', 'alice']
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame(data)

    # Perform some data processing using Pandas
    # For example, let's say we want to calculate the sum of a column
    sum_value = df['sal'].sum()

    # Print the result
    print("Sum of column 'column_name':", sum_value)


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'pandas_data_processing',
    default_args=default_args,
    description='A simple DAG to process data using Pandas',
    schedule_interval='@daily',  # Run the DAG daily
)

# Define the PythonOperator to execute the data processing function
process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag,
)

# Define the task dependencies
process_data_task
