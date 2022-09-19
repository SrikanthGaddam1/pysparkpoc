from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
"""data= "C:\\bigdata\\datasets\\asl.csv"
df=spark.read.format("csv").option("header","true").load(data)
df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where age<=30")
res=df.where(col("age")>=30)
res.show()"""
#rdd api
data= "C:\\bigdata\\datasets\\asl.csv"
ardd=spark.sparkContext.textFile(data)
res= not ardd.filter(lambda x: "age" not in x).map(lambda x: x.split(","))
for x in res.collect():
     print(x)
