from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

dag = DAG(
    'spark_job_dag_2',
    default_args=default_args,
    description='DAG to run PySpark job run daily on EMR from 8th march',
    
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
spark_step1 = {
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



submit_job1_task = EmrAddStepsOperator(
    task_id='submit_job1',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[spark_step1],
    dag=dag,
)

#This task is a sensor that waits for a specific step in the EMR cluster to complete. 
sensor1_task = EmrStepSensor(
    task_id='watch_step_1',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_job1', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)


spark_step2 = {
    'Name': 'PySpark filter people data',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['spark-submit',
                 '--deploy-mode', 'cluster',
                 '--master', 'yarn',
                 's3://emrbucket13/script/process_data.py']
    }
}

submit_job2_task = EmrAddStepsOperator(
    task_id='submit_job2',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    steps=[spark_step2],  # Define steps to execute PySpark program 2
    dag=dag,
)



#This task is a sensor that waits for a specific step in the EMR cluster to complete. 

sensor2_task = EmrStepSensor(
    task_id='watch_step_2',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_job2', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)


# Optionally add cluster termination:
terminate_cluster_task = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)


create_job_flow_task >> submit_job1_task >> sensor1_task >> submit_job2_task >> sensor2_task >> terminate_cluster_task





