from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
'''data="C:\\bigdata\\datasets\\asl.csv"
df=spark.read.format("csv").option("header","true").load(data)
#df.show()
df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where city == 'blr' ")
res=df.where((col("city")== "blr") | (col("age") >= 30))

res.show()'''

#Rdd Api
data="C:\\bigdata\\datasets\\asl.csv"
ardd= spark.sparkContext.textFile(data)
#res=ardd.filter(lambda x:"city" not in x).map(lambda x:x.split(",")).filter(lambda x: "blr" in x)
res=ardd.filter(lambda x:"city" not in x).map(lambda x: x.split(",")).filter(lambda x: int(x[1])>=30)
for x in res.collect():
    print(x)

