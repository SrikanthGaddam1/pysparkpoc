from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
import sys
import os

#data="C:\\bigdata\\datasets\\ca-500.csv"
data=sys.argv[1]
op=sys.argv[2]
df=spark.read.format("csv").option("inferSchema","true").option("header","true").option("sep",";").load(data)
df.createOrReplaceTempView("catab")
res=spark.sql("select* from catab where email like '%gmail.com%' province='QC'")
res.show()
res.write.format("csv").option("header","true").save(op)
#learing...
#Dovelopment..
#(Benifit show) testing...
#production...(env)


# File location and type
'''file_location = "C:\\bigdata\\datasets\\ca-500.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df.show()'''
