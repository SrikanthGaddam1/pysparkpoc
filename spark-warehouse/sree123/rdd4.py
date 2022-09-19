from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\emailsmay4.txt"
rdd=sc.textFile(data)
#res=rdd.filter(lambda x:"gmail" in x).flatMap(lambda x:x.split(" ")).filter(lambda x:"@" in x)
#map: ['Praveenkumar', 'S', '(Private):', '8:43', 'PM:', 'Sathyapraveen.sk@gmail.com']
res=rdd.filter(lambda x:"gmail" in x).map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1]))
#for i in res.take(11):
#    print(i)
df=res.toDF(["name","email"])
df.show()
