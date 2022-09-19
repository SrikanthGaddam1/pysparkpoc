from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

data="C:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("inferSchema","true").option("header","true").option("sep",",").load(data)
def stoff(st):
    if(st=="OH" or st=="NJ" or st=="CA"):
        return "20% off"
    elif(st=="LA" or st=="NY" or st=="AK"):
        return "10% off"
    else:
        return "100 rupees off"
#above python function convert to udf.
#spark able to understand only udf not customized python/scala functions.
uoff = udf(stoff)
#i want to run this udf in spark sq. at that time ur giving one name to that udf .. using below syntax.
spark.udf.register("offer",uoff)


df.createOrReplaceTempView("tab")
ndf=df.withColumn("phone1", regexp_replace(col("phone1"),"-",""))\
    .withColumn("email", when(col("email").like("%cox%"),"this mail banned").otherwise(col("email")))
#ndf=spark.sql("select *, offer(state) weekedoffer from tab ")
#ndf=df.withColumn("offer", uoff(col("state")))
ndf.show()






