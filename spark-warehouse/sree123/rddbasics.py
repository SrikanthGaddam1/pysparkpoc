from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
#rdd: collection of java objects called RDD. rdd follows immutability, fault tolerance and laziness properties
#two ways to create rdd. 1) sc.parallelize() used to create rdd from scala/java/python eleemnts
#2) sc.textFile() used to create rdd from external storage use it.
lst=[1,2,4,5,9,11,15,12]
#convert to rdd
lrdd=sc.parallelize(lst)

res=lrdd.map(lambda x:x*x).filter(lambda x:x<100)
for x in res.collect():
    print(x)

    #A function or method feel lazy to process called transformations. It return rdd
    #map filter, feel lazy so transformation

    #A function or method doesn't feel lazy to process called action. It return results/value
    #collect: return results so its action.
