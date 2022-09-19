from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\bigdata\\datasets\\asl.csv"
ardd=sc.textFile(data)
skip=ardd.first() #return first line (name,age,city)
#res=ardd.filter(lambda x: "hyd" in x)
#res=ardd.map(lambda x:x.split(",")).filter(lambda x: "blr" in x[2])
res=ardd.filter(lambda x:x!=skip).map(lambda x:x.split(","))
    #filter((lambda x: int(x[1])>32) and (lambda x: "blr" in x[2]))
for x in res.collect():
    print(x)