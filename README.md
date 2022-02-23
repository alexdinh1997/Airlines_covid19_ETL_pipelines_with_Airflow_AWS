# ETL PIPELINES ON AIRFLOW, SPARK AND EMR

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

### INITIALIZE ENIVIRONMENT:
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
