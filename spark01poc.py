#cleaning schema data & datacleaning
from pyspark.sql.functions import *
from pyspark.sql import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\empdata.txt"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
cols=[re.sub('[^a-zA-Z0-9]',"",c.upper()) for c in df.columns]
res=df.toDF(*cols)
#  res.show()
res1=res.select([regexp_replace(col(x),"\"","").alias(x) for x in res.columns])
res.printSchema()
res1.show()