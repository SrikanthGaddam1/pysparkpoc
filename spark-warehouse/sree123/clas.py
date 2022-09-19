from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
#dataframe api
data="C:\\bigdata\\datasets\\asl.csv"
df=spark.read.format("csv").option("header","true").load(data)
df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where city='blr'")
res=df.where(col("age")>=30)
res.show()
#Rdd api
"""data="C:\\bigdata\\datasets\\asl.csv"
ardd = spark.sparkContext.textFile(data)
fst=ardd.first()
#res = ardd.filter(lambda x:"age" not in x).map(lambda x:x).filter(lambda x: "blr" in x)
res = ardd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).filter(lambda x:int(x[1])>=30)

for x in res.collect():
    print(x)"""