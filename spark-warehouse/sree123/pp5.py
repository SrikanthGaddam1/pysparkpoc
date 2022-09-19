from pyspark.sql.functions import *
from pyspark.sql import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("infraSchame","true").load(data)
#df.createOrReplaceTempView("tab")
scma=[re.sub('[^a-zA-Z]',"",a.lower()) for a in df.columns]
res=df.toDF(*scma)

res1=res.withColumn("age",lit(123)).withColumn("phoneno",regexp_replace(col("phoneno"),"-",""))\
    .withColumn("ssn",regexp_replace(col("ssn"),"-",""))\
    .withColumn("fullname",concat_ws(" ",col("nameprefix"),col("firstname"),col("middleinitial"),col("lastname")))\
    .withColumnRenamed("lastname","lname").withColumn("dateofbirth",to_date("dateofbirth","M/d/yyyy"))\
    .withColumn("dateofjoining",to_date("dateofjoining","M/d/yyyy")).withColumn("dateyoung",datediff(col("dateofjoining"),col("dateofbirth")))\
    .orderBy(col("dateyoung").asc()).withColumn("dateyoung",col("dateyoung")/365)\


'''res1=res.withColumn("age",lit("990"))\
    .withColumn("phoneno", regexp_replace(col("phoneno"),"-",""))\
    .withColumn("fullname",concat_ws(" ","nameprefix","firstname","middleinitial","lastname"))\
    .withColumn("dateofbirth",to_date("dateofbirth","M/d/yyyy"))\
    .withColumn("dateofjoining",to_date("dateofjoining","M/d/yyyy"))\
    .withColumn("dateyoung",datediff(col("dateofjoining"),col("dateofbirth"))).orderBy(col("dateyoung").asc())\
    .withColumn("dateyoung",col("dateyoung")/365)'''
res5=res1.groupBy(col("gender")).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())


res3=res1.groupBy(col("county")).agg(count("*").alias("cnt")).orderBy(col("cnt").asc())



res2=res1.select(sorted(res1.columns))

res1.show(truncate=True)
res1.printSchema()
res5.show()
