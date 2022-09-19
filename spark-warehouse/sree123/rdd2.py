from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\donations.csv"
rdd=sc.textFile(data) 
fst=rdd.first()
res=rdd.filter(lambda x:x!=fst).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y)
for i in res.take(9):
    print(i)
