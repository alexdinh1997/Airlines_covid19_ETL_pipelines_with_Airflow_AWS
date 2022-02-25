# ETL PIPELINES ON AIRFLOW, SPARK AND EMR

![process](https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/Local.png)


## I. SETTING UP AIRFLOW ON DOCKER
1. Fetch the `docker-compose.yml` file:
 
 `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.1/docker-compose.yaml'`

Metainfo:
- `airflow-schedule`: The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
- `airflow-webserver`: webserver at `http://localhost:8080`
- `airflow-worker`: The wo rker that executes the tasks given by the scheduler.
- `airflow-init`: The initialization service.
- `flower`: The flower app for monitoring the environment. It is available at http://localhost:8080.
- `postgres`: Database
- `redis`: The redis - broker that forwards messages from scheduler to worker.

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.

- ./dags - you can put your DAG files here.
- ./logs - contains logs from task execution and scheduler.
- ./plugins - you can put your custom plugins here.

## II. INITIALIZE ENIVIRONMENT:
- Before running **Airflow** on **Docker** for the first time, we need to create files and directories inside the working main directory and initialize the database.

~~~
mkdir ./dags ./plugins ./logs
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
~~~

- Run the database and first time create a user
`docker-compose up airflow-init`
After initialization is complete, you should see a message like below.


~~~
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.2.3
start_airflow-init_1 exited with code 0
~~~
The account created has the login airflow and the password airflow.
## III. SETTING UP AWS ROLES AND SECURITY GROUP:
1. Create keypair for ec2 instances
(add info)
2. Create S3 bucket
(add info)
3. Spark job and and spark step for EMR cluster
(add info)
4. Add environment to cluster
(add info)
## IV. RUNNING AND SETTING UP CONNECTIONS ON AIRFLOW UI:

<img src="https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/dir-tree.png" width=75% height=75%>

To run the docker container, we have 2 ways to proceed. 1 is the turn on the docker Dashboard, 2 is command in Terminal 
`docker-compose up` to run the docker-compose.yml file.
- in http://localhost:8080 in **Admin** session, go to **Connections** and modify **EMR_default** and **AWS_default**
- in **AWS_default**, we setup **login** and **password** respectively as IAM_role_name and Secret_key we have been set before and set the **Extra_path** with region as us-west2
- in **EMR_defaut**, following the SPARK_JOB in the DAG.py file, we delete the original extra JSON file and leave it at blank.
- Go to 

## V. DATA INSPECTION
## VI. MODELLING DATA
## VII. AIRFLOW USAGES
## VIII. QUALITY CHECK
## IX. RESULT

-----------
Credits:
### Setup the airflow with docker
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
### 
