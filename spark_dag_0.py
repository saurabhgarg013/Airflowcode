from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 4, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG to run PySpark job run daily on EMR from 14th march',
    
)

#Create an EC2 Key Pair: If you haven't already created an EC2 key pair, 

JOB_FLOW_OVERRIDES={
    'Name': 'Spark Cluster',
    'LogUri': 's3://emrbucket13/logs/',
    'ReleaseLabel': 'emr-7.0.0',
     "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'Ec2KeyName': 'your-key-pair',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


#"EMR_EC2_DefaultRole","EMR_DefaultRole"
 
create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag,
)






# Define PySpark step
spark_step = {
    'Name': 'PySpark Word Count',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['spark-submit',
                 '--deploy-mode', 'cluster',
                 '--master', 'yarn',
                 's3://emrbucket13/script/word_count.py']
    }
}



add_step_task = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[spark_step],
    dag=dag,
)


#This task is a sensor that waits for a specific step in the EMR cluster to complete. 
step_sensor_task = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

create_job_flow_task >> add_step_task >> step_sensor_task
