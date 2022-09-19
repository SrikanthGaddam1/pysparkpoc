from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\bigdata\\datasets\\donations.csv"
ardd=sc.textFile(data)
skip=ardd.first()
pro=ardd.filter(lambda x:x!=skip).map(lambda x:x.split(","))\
    .map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x, y: x+y).sortBy(lambda x:x[1],False)
#reduceByKey .. based on key it process value. Means ('venu', 2000x) ('venkat', 4000) ('venu', 2000y) .. if u have data like that .. venu ..


for x in pro.take(15):
    print(x)
    #collect is a action it return all emements to driver