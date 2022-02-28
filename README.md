# APPLICATION OF AIRFLOW ON ETL PIPELINE ORCHESTRATION IN EMR CLUSTERS

## PROJECT DISCRIPTION:

## SENARIO:

The goal of the data engineering capstone project is to allow you to use what you've learnt during the curriculum. This project will be a significant component of your portfolio, assisting you in achieving your data engineering career objectives. 

In this project, you have the option of completing the project that has been assigned to you or defining the scope and data for a project of your own design. In any case, you'll be required to follow the instructions specified below.

## BRIEF OUTLINE:

## PROJECT ARCHITECHTURE
1. Overall architechture

![process](https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/Local.png)

2. EMR architechture

<img src="https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/EMR%20nodes.png" width=50% height=50%>

## PROJECT PHASES
1. Phase 1:
2. Phase 2:
3. Phase 3:

## SETTING UP AIRFLOW ON DOCKER
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

## INITIALIZE ENIVIRONMENT:
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
## SETTING UP AWS ROLES AND SECURITY GROUP:
1. Create keypair for ec2 instances
(add info)
2. Create S3 bucket
(add info)
3. Spark job and and spark step for EMR cluster
(add info)
4. Add environment to cluster
(add info)
## DAG ARCHITECHTURE

<img src="https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/DAG-Diagram.png">

## RUNNING AND SETTING UP CONNECTIONS ON AIRFLOW UI:

<img src="https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/dir-tree.png" width=75% height=75%>

To run the docker container, we have 2 ways to proceed. 1 is the turn on the docker Dashboard, 2 is command in Terminal 
`docker-compose up` to run the docker-compose.yml file.
- in http://localhost:8080 in **Admin** session, go to **Connections** and modify **EMR_default** and **AWS_default**
- in **AWS_default**, we setup **login** and **password** respectively as IAM_role_name and Secret_key we have been set before and set the **Extra_path** with region as us-west2
- in **EMR_defaut**, following the SPARK_JOB in the DAG.py file, we delete the original extra JSON file and leave it at blank.
- Go to 

## DATA INSPECTION
### 1. Context

COVID-19 has severely crippled the global airline industry with air service reductions widespread throughout 2020. This dataset containing ~ 11 million flights will aid those seeking to visualize the impact that the virus has had on the domestic United States airline industry through detailed flight delay and cancellation data.

### 2. Content

The United States Department of Transportation's (DOT) Bureau of Transportation Statistics tracks the on-time performance of domestic flights operated by large air carriers. The data collected is from January - June 2020 (will be updated soon) and contains relevant flight information (on-time, delayed, canceled, diverted flights) from the Top 10 United States flight carriers for 11 million flights. This dataset includes more than 2.7 milions rows of data in jan2jun.csv

Note: Data is in 47 columns with full column descriptions in the attached .txt file.

### 3. Acknowledgements

The flight delay and cancellation data were collected from the U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics.


In this dataset, it includes 2
## MODELLING DATA
1. Entity table

![Entity_relationship](https://github.com/alexdinh1997/Airlines_covid19_ETL_pipelines_with_Airflow_AWS/blob/master/img/entity_relationship.PNG)

2. Schema dictionary

## AIRFLOW USAGES
## QUALITY CHECK
## RESULT
## FINANCIAL SUMMARY
1. EMR Cluster
~~~
Pricing based on US-West-2 pricing.

Suppose you run an Amazon EMR application deployed on Amazon EC2, and that you use one m5.xlarge EC2 instance as your master node and two m5.xlarge EC2 instances as core nodes. You will be charged for both EMR and for the EC2 nodes. If you run for one month, with 100% utilization during that month, and use on-demand pricing for EC2, your charges will be:

Master node:

EMR charges = 1 instance(s) x 0.048 USD hourly x (100 / 100 Utilized/Month) x 730 hours in a month = 35.0400 USD (EMR master node cost)
EMR master node cost (monthly): 35.04 USD

Core nodes:

EMR charges = 2 instance(s) x 0.048 USD hourly x (100 / 100 Utilized/Month) x 730 hours in a month = 70.0800 USD (EMR core node cost)
EMR core node cost (monthly): 70.08 USD
~~~
2. EC2 instances
~~~
EC2 charges = 3 instances x 0.204 USD x 730 hours in a month = 446.76 USD (monthly onDemand cost)
Amazon EC2 On-Demand instances (monthly): 446.76 USD
~~~
3. S3 bucket

a. S3 standard

**Unit conversions**
~~~
S3 Standard storage: 20 TB per month x 1024 GB in a TB = 20480 GB per month
Data returned by S3 Select: 5 TB per month x 1024 GB in a TB = 5120 GB per month
~~~
**Pricing calculations**
~~~
Tiered price for: 20480 GB
20480 GB x 0.0230000000 USD = 471.04 USD
Total tier cost = 471.0400 USD (S3 Standard storage cost)
10,000 PUT requests for S3 Storage x 0.000005 USD per request = 0.05 USD (S3 Standard PUT requests cost)
50,000 GET requests in a month x 0.0000004 USD per request = 0.02 USD (S3 Standard GET requests cost)
5,120 GB x 0.0007 USD = 3.584 USD (S3 select returned cost)
5 GB x 0.002 USD = 0.01 USD (S3 select scanned cost)
471.04 USD + 0.02 USD + 0.05 USD + 3.584 USD + 0.01 USD = 474.70 USD (Total S3 Standard Storage, data requests, S3 select cost)
S3 Standard cost (monthly): 474.70 USD
~~~
b. Data transfers

**Unit conversions**
~~~
Inbound:
Internet: 5 TB per month x 1024 GB in a TB = 5120 GB per month
Outbound:
Internet: 5 TB per month x 1024 GB in a TB = 5120 GB per month
~~~
**Pricing calculations**
~~~
Inbound:
Internet: 5120 GB x 0 USD per GB = 0.00 USD
Outbound:
Internet: 5120 GB x 0.09 USD per GB = 460.80 USD
Data Transfer cost (monthly): 460.80 USD

S3 charges = 474.70 USD + 460.80 USD = 935.50 USD
~~~

**Total charges =35.04 USD + 35.04 USD + 446.76 USD + 935.50 USD = 1,496.78 USD (Total 12 months cost: 17,961.36 USD, Includes upfront cost)**


## Credits:
### Setup the airflow with docker
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
### AWS calculation
https://calculator.aws/#/

