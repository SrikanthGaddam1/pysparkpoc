from pyspark.sql.functions import *
from pyspark.sql import *
import os
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# Set options below
sfOptions = {
  "sfURL" : "ld19496.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "SRIKANTHGADDAM4",
  "sfDatabase" : "srikanthdb",
  "sfPassword": "Kanth@6688",
  "sfSchema" : "public",
  "sfWarehouse" : "smallcl"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from BANKTAB") \
  .load()

df.show()