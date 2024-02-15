import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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

job.commit()