from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
#python objects
data=[1,5,4,32,44,11]

#python object convert to rdd
drdd=sc.parallelize(data)
mul= (lambda x:x*x)
#pro = drdd.map(lambda x:x*x)
pro = drdd.map(mul)

for x in pro.collect():
    print(x)