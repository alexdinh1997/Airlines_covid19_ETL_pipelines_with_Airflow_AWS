import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
from datetime import datetime, timedelta
from pyspark import SparkConf,SparkContext
conf = SparkConf()
sc = SparkContext(conf = conf)

class create_dim:
    # extract from metadata .txt files: airlines and CANCELLATION_CODE

    @staticmethod
    def create_airline_table(path, spark):
        """
        Function: Generate and create airline table code
        param: 
            - path: txt file
        output: airline df
        """
        f = sc.textFile(path)
        content = f.collect()
        content = [x.strip() for x in content]
        #strip(): removes any leading (spaces at the beginning) and trailing (spaces at the end)
        #characters (space is the default leading character to remove)
        
        airline = content[10:20]
        splitted_airline = [c.split(":") for c in airline]
        c_airline = [x[0].replace("'","").strip() for x in splitted_airline]
        airline_name = [x[1].replace("'","").strip() for x in splitted_airline]
        airline_df = spark.createDataFrame(zip(c_airline, airline_name), schema=['c_airline', 'airline_name'])
        return airline_df
        
    @staticmethod
    def create_cancellation_table(path, spark):
        """
        Function: Generate and create Cancelation_code table:
        param: Path of datafile
        input: .txt file
        output: cancel.csv file stored in data folder
        """
        f = sc.textFile(path)
        content = f.collect()
        content = [x.strip() for x in content]
        cancel = [re.search('\(([^)]+)', content[49]).group(1)][0].split(",")
        splitted_cancel = [c.split(":") for c in cancel]
        c_cancel = [x[0].replace("'","").strip() for x in splitted_cancel]
        cancel_des= [x[1].replace("'","").strip() for x in splitted_cancel]
        c_cancel.append('O')
        cancel_des.append('Non-cancel')
        cancel_df = spark.createDataFrame(zip(c_cancel, cancel_des), schema=['c_cancel', 'cancel_des'])
        return cancel_df

    @staticmethod
    def create_port_loc_table(path, spark):
        """
        Function: Generate and create port location table:
        param: Path of datafile
        output: port_location file
        """
        df = spark.read.csv(path, header=True)
        for column in df.columns:
            df = df.withColumnRenamed(column, column.lower())
        port_loc_df = df.select('origin', 'origin_city_name', 'origin_state_abr').dropDuplicates()
        port_loc_df = port_loc_df.withColumn('origin_city_name', split(port_loc_df['origin_city_name'], ',').getItem(0))
        return port_loc_df

    @staticmethod
    def create_distance_group_table(spark):
        """
        Function: Create distance group for flights
        target: set standard for distance scale for lights
        output: distance range dataset
        """
        data = []
        for i in range(26):
            data.append([i, "{} <= distance < {}".format(i * 250, (i + 1) * 250)])

        df = spark.createDataFrame(data, schema=['distance_group', 'distance_range_in_miles'])
        return df

    @staticmethod
    def create_states_table(path, spark):
        """
        Function: Create states table
        param: Path of data file
        output: state_df

        """
        df = spark.read.csv(path, header=True)
        for column in df.columns:
            df = df.withColumnRenamed(column, column.lower())
        state_df = df.select('origin_state_abr', 'origin_state_nm').dropDuplicates()
        return state_df


    @staticmethod
    def create_delay_group_table(spark):
        """
        Function: Create delay group table
        param: Path of data file
        output: delay_group df
        """
        data = []
        for i in range(-1,188):
            if i == -1:
                data.append([-1,"Early"])
            elif i == 0:
                data.append([0,"On Time"])
            else:
                data.append([i, "{} <= delay time < {}".format(i * 15, (i + 1) * 15)])
        df = spark.createDataFrame(data, schema=['delay_group', 'delay_time_range_in_min'])
        return df


class clean:
    @staticmethod
    def clean_fact_table(path, spark):
        """
        Function: 
        Param: 
        Output: 
        """
        string_to_date= udf(lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
        """
        Function: Date conversion for a date string col
        Param: date Columns
        Output: a date columns with type of date
        """
        fact = spark.read.csv(path, header=True)
        fact = fact.withColumn('flight_id', row_number().over(Window.orderBy(monotonically_increasing_id()))-1)\
                    .withColumn('Flight_date', string_to_date(col('FL_DATE')))\
                    .withColumnRenamed("MKT_UNIQUE_CARRIER","c_airline")\
                    .withColumn("Flight_num",col("MKT_CARRIER_FL_NUM").cast("int"))\
                    .withColumn("CRS_arr_time",lpad(col("CRS_ARR_TIME"),4,'0'))\
                    .withColumn("CRS_dep_time",lpad(col("CRS_DEP_TIME"),4,'0'))\
                    .withColumn("Dep_time",lpad(col("DEP_TIME"),6,'0'))\
                    .withColumn("Arr_time",lpad(col("ARR_TIME"),6,'0'))\
                                .withColumn("CRS_dep_time",date_format(to_timestamp("CRS_dep_time",'HHmm'),"HH:mm"))\
                        .withColumn("CRS_arr_time",date_format(to_timestamp("CRS_arr_time",'HHmm'),"HH:mm"))\
                    .withColumn("Dep_time",date_format(to_timestamp("Dep_time",'HHmm.0'),"HH:mm"))\
                    .withColumn("Arr_time",date_format(to_timestamp("Arr_time",'HHmm.0'),"HH:mm"))\
                    .withColumn("DEP_DELAY_GROUP",col("DEP_DELAY_GROUP").cast("int"))\
                    .withColumn("ARR_DELAY_GROUP",col("ARR_DELAY_GROUP").cast("int"))\
                    .withColumn("c_cancellation",when(col("cancellation_code").isNull(),"O").otherwise(col("cancellation_code")))\
                    .withColumn("Distance_group",floor(col("DISTANCE").cast("int")/250))\
                    .drop("cancellation_code","MKT_CARRIER_FL_NUM")

        return fact.select("flight_id","Flight_date","C_airline","Flight_num","CRS_ARR_time","CRS_DEP_time","DEP_time","ARR_time","DEP_delay_group","ARR_delay_group","Origin","Dest","C_cancellation","Distance_group").dropna(subset= ["DEP_delay_group","ARR_delay_group"])



 # create session

def create_spark_session():
    """
    Function: Create spark session
    """
    spark = SparkSession\
        .builder \
        .enableHiveSupport().appName("Create schema flight-during-covid19 data") \
        .getOrCreate()
    return spark

def process_data(spark,input,output):
    """
    Function: ETL pipeline establishment
    param:
        - spark: Spark session
        - input: input file in HDFS
        - output: output file to s3
    """
    input1 = f'{input}/jantojun2020.csv'
    input2 = f'{input}/ColumnDescriptions.txt'
    airline_df = create_dim.create_airline_table(input2,spark)
    airline_df.write.parquet(f"{output}/airline_dim.parquet",mode="overwrite")
    cancel_df = create_dim.create_cancellation_table(input2,spark)
    cancel_df.write.parquet(f"{output}/cancel_dim.parquet",mode="overwrite")
    port_loc_df = create_dim.create_port_loc_table(input1,spark)
    port_loc_df.write.parquet(f"{output}/port_loc_dim.parquet",mode="overwrite")
    dist_group_df = create_dim.create_distance_group_table(spark)
    dist_group_df.write.parquet(f"{output}/dist_group_dim.parquet",mode="overwrite")
    states_df = create_dim.create_states_table(input1,spark)
    states_df.write.parquet(f"{output}/states_dim.parquet",mode="overwrite")
    delay_group_df = create_dim.create_delay_group_table(spark)
    delay_group_df.write.parquet(f"{output}/delay_group_dim.parquet",mode="overwrite")
    fact_df= clean.clean_fact_table(input1,spark)
    fact_df.write.parquet(f"{output}/fact_dim.parquet",mode="overwrite")
   
def main():
    """
    Operating function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",type=str,help="HDFS input",default="/flight")
    parser.add_argument("--output",type=str,help="HDFS output",default="/output")
    args = parser.parse_args()

    spark = create_spark_session()
    
    process_data(spark,input=args.input, output=args.output)
    #input=args.input

if __name__ == "__main__":
    main()


