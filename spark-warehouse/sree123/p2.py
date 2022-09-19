from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
import re
data="C:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
cols=[re.sub('[^a-zA-Z]',"",c.lower()) for c in df.columns]
#toDf is used to rename all columns
ndf=df.toDF(*cols)
'''withcolum used to add a new column if that column not exists.
   withcolumn used to update column values if that column alredy exists.
    lit ... used to add dummy data'''
res=ndf.withColumn("age",lit(18)).withColumn("phoneno",regexp_replace(col("phoneno"),"-","")).withColumnRenamed("age","sage")\
    .withColumn("email",regexp_replace(col("email"),"gmail","ggggg"))
#res1=res.select([upper(col(x)).alias(x) for x in res.columns])
res1=res.select([regexp_replace(col(x),"\.","*").alias(x) for x in res.columns])

#res=ndf.withColumn("fullname").groupBy("nameprefix","firstname","middleinitial","lastname")

res1.show(truncate=False)
