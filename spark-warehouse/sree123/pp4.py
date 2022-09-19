from pyspark.sql.functions import *
from pyspark.sql import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchama","true").load(data)
cols=[re.sub("[^a-zA-Z]","",a.lower()) for a in df.columns]
ndf=df.toDF(*cols)
res=ndf.withColumn("age",lit("19")).withColumnRenamed("age","mage").withColumn("ssn", regexp_replace(col("ssn"),"-","")).\
    withColumn("fullname",concat_ws(" ",col("nameprefix"),col("firstname"),col("middleinitial"),col("lastname")))\
    .drop("nameprefix","firstname","middleinitial" ,"lastname")\
    .withColumn("dateofbirth",to_date(col("dateofbirth"),"M/d/yyyy"))\
    .withColumn("dateofjoining", to_date(col("dateofjoining"),'M/d/yyyy'))\
    .withColumn("youngage",datediff(col("dateofjoining"),col("dateofbirth")))\
    .orderBy(col("youngage").asc())


res1=res.select(sorted(res.columns))



#res1=res.select([upper(col(a)).alias(a) for a in res.columns ])
#res1=res.select([regexp_replace(col(x),"\.","#").alias(x) for x in lastnameres.columns])
res2.show()
