from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\bigdata\\datasets\\email.txt"
ardd=sc.textFile(data)

res=ardd.flatMap(lambda x:x.split(" ")).filter(lambda x: "@" in x)\
    .map(lambda x:x.split("@")).map(lambda x:(x[1].lower(),1)).reduceByKey(lambda x,y:x+y)

for x in res.collect():
    print(x)