from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc= spark.sparkContext
data="C:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferschama","true").load(data)
res=df.where((col("state")=="NY") & (col("emai").like("%gmail%")))
res.show()
#df.show()
