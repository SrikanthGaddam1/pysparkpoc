
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
#data=[12,32,34,4,54,26]
#drdd=spark.sparkContext.parallelize(data)
data="C:\\bigdata\\datasets\\asl.csv"
aslrdd=sc.textFile(data)

#res=aslrdd.map(lambda x:x.split(","))
#res=aslrdd.map(lambda x:x.split(",")).filter(lambda x: "blr" in x[2])
res=aslrdd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).filter(lambda x: int(x[1])>=30)
#.toDF(["name","age","city"])
#res.createOrReplaceTempView("tab")
#res.show()

#filter by default apply a logic /filter on top of entire line
#filter almost in sql ur using where condition to filter results similarly ur using filter function to filter values.

for i in res.collect():
    print(i)
