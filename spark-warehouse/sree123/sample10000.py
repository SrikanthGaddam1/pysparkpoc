import re

from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc= spark.sparkContext
data="C:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferschama","true").load(data)
cols=[re.sub('[^a-zA-Z]','', x.lower()) for x in df.columns]
#toDF used to rename all columns
ndf=df.toDF(*cols)
ndf.createOrReplaceTempView("tab")
res=spark.sql("select * from tab")
res.show()