# import libraries
# These liraries for S3 modify
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# Libraries for EMR creation
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
#libraries for step of EMR and terminate the job
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
#import os
import logging

from datetime import datetime,timedelta

### Define dag
default_args = {
    'owner': 'Alex',
    'start_date': datetime(2022, 2, 15),
    'depends_on_past': False,
    'catchup': False
}

dag = DAG('Airflow_and_EMR',
          default_args=default_args,
          description='Load and transform data in EMR with Airflow',
          schedule_interval='@once'
        )

### General Configurations
# random text classification is the naive pyspark script that read the 
BUCKET_NAME = "flight-thesis-covid19"
s3_csv = "data_files/jantojun2020.csv"
s3_text = "data_files/ColumnDescriptions.txt"
s3_script = "scripts/random_text_classification.py"
s3_clean = "output/"
local_csv = "./dags/data/jantojun2020.csv"
local_text = "./dags/data/ColumnDescriptions.txt"
local_script="./dags/scripts/spark/random_text_classification.py"

### Setup EMR cluster configurations and operations used
JOB_FLOW_OVERRIDES={
    "Name":"Flight-cancelation-during-covid19",
    "ReleaseLabel":"emr-5.29.0",
    # In this EMR cluster, we need HDFS and Spark
    "Applications":[{"Name":"Hadoop"},{"Name":"Spark"}],
    "Configurations":[
        {
        "Classification":"spark-env",
        "Configurations":[
        {
            "Classification":"export",
            "Properties":{"PYSPARK_PYTHON":"/usr/bin/python3","JAVA_HOME": "/usr/lib/jvm/java-1.8.0"},
                }
            ],
        }
    ],
    "Instances":{
        "InstanceGroups":[
            {
                "Name":"Master node",
                "Market":"ON_DEMAND",
                "InstanceRole":"MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount":1,
            },
            {
                "Name": "Core Nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        # This lets us programmatically terminate the cluster
        "KeepJobFlowAliveWhenNoSteps":True,
        "TerminationProtected":False,
        "Ec2KeyName": "spark", 
    },
    "JobFlowRole":"EMR_EC2_DefaultRole",
    "ServiceRole":"EMR_DefaultRole",
    "LogUri":"s3://flight-thesis-covid19/job/",
    "VisibleToAllUsers":True
}

#Create an EMR CLuster
create_emr_cluster =EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides = JOB_FLOW_OVERRIDES,
    aws_conn_id ="aws_default",
    emr_conn_id="emr_default",
    dag =dag,
)

### EMR Spark Step
SPARK_STEPS =[
    {
        "Name":"Move raw data to S3 to HDFS",
        "ActionOnFailure":"CANCEL_AND_WAIT",
        "HadoopJarStep":{
            "Jar":"command-runner.jar",
            "Args":[
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data_files",
                "--dest=/flight",
            ],
        },
    },
    {
        "Name":"ETL pipelines",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]

# helper function
# def _local_to_s3(filepath,key,bucket_name=BUCKET_NAME):
#     """
#     Function: Loading local file to S3 bucket.
#     Param:
#         - filename: source file of input
#         - key folder/key
#         - bucketname: S3 available bucket
#     Output:
#         Return nothing, check on S3 dashboard if all the files was uploaded.
#     """
# def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
#     s3 = S3Hook()
#     s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
    # s3 = S3Hook(aws_conn_id="aws_default")
    # for root, dirs, files in os.walk(f"{os.getcwd()}/{filepath}"):
    #     for filename in files:
    #         file_path = f"{root}/{filename}"
    #         s3.load_file(
    #             filename=file_path, key=f"{key}{filename}", bucket_name=bucket_name, replace=True)

### DAG defines
start_operator = DummyOperator(
    task_id ="Begin_execution",
    dag=dag
)
# csv_to_s3 = PythonOperator(
#     dag=dag,
#     task_id = "csv_to_s3",
#     python_callable = _local_to_s3,
#     op_kwargs = {"filename":local_csv,"key":s3_csv,}
# )
# txt_to_s3 = PythonOperator(
#     dag=dag,
#     task_id="txt_to_s3",
#     python_callable = _local_to_s3,
#     op_kwargs = {"filename":local_text,"key":s3_text, }
# )
# scripts_to_s3 = PythonOperator(
#     dag=dag,
#     task_id="scripts_to_s3",
#     python_callable = _local_to_s3,
#     op_kwargs = {"filename":local_script,"key":s3_script, }
# )

#Adder step
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    # these params are used to fill the paramterized values in SPARK_STEPS json
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_csv": s3_csv,
        "s3_text": s3_text,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)
# temrinate the cluster once steps/tasks are completed 
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_operator = DummyOperator(
    task_id ="end_pipeline",
    dag=dag,
)

#start_operator >> [csv_to_s3,scripts_to_s3,txt_to_s3] >> create_emr_cluster
#start_operator >> scripts_to_s3 >> create_emr_cluster
#start_operator >> txt_to_s3 >> create_emr_cluster
start_operator >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_operator