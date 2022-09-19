from pyspark.sql.functions import *
from pyspark.sql import *
import os
#spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
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
  .option("query",  "select * from custspent1") \
  .load()

df.show()

#ohost="jdbc:oracle:thin:@//chakridb.cpm0rg3ian3a.ap-south-1.rds.amazonaws.com:1521/ORCL"
     # .option("password","opassword").option("dbtable","empp").option("driver","oracle.jdbc.driver.OracleDriver").load()
#df.write.mode("overwrite").format("jdbc").option("url",ohost).option("user","ouser")\
#.option("password","opassword").option("dbtable","deptmysql").option("driver","oracle.jdbc.driver.OracleDriver").save()