import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext() # SparkContext is the entry point to any Spark functionality.
glueContext = GlueContext(sc) # GlueContext is a class provided by AWS Glue that extends the functionality of SparkContext.
spark = glueContext.spark_session # SparkSession is the entry point to Spark SQL functionality and the DataFrame API.
job = Job(glueContext)   # Job is a class provided by AWS Glue for defining and running ETL (Extract, Transform, Load) jobs.
job.init(args['JOB_NAME'], args) # The init method is used to initialize the job

df1 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://year2022-2023.c6lgb9aybcpq.us-east-1.rds.amazonaws.com:3306/group52") \
    .option("dbtable", "data2020") \
    .option("user", "admin") \
    .option("password", "qwertyuiop") \
    .load()
df2 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://year2022-2023.c6lgb9aybcpq.us-east-1.rds.amazonaws.com:3306/group52") \
    .option("dbtable", "parking_violations05") \
    .option("user", "admin") \
    .option("password", "qwertyuiop") \
    .load()

df3 = spark.read.csv("s3://nycparkingtickets/year20212223/",header=True)
df4 = df1.union(df2)
df5 = df4.union(df3)
print("Dataframe is loaded")
df5.coalesce(1).write.parquet("s3://nycgroup05datalake/fiveyearsnycdata")
df6 = spark.read.csv("s3://nycparkingtickets/parking_violation_codes (1).csv",header=True)
df6.write.parquet("s3://nycgroup05datalake/smalltable")

job.commit()