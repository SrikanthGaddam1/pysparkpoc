from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext

sfOptions = {
  "sfURL" : "ii41113.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "SRIKANTHBABUGADDAM",
  "sfPassword" : "Spark@123",
  "sfDatabase" : "SRIDB",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

#df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query",  "select * from CUSTOMER") .load()

#df.show()
data="C:\\bigdata\\datasets\\us-500.csv"
df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df1.show()
df1.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "us500").mode("append").save()
