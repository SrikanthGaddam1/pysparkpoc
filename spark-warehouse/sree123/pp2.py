from pyspark.sql.functions import *
from pyspark.sql import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchama","true").load(data)
cols=[re.sub('[^a-zA-Z]',"",a.lower()) for a in df.columns]
ndf=df.toDF(*cols)
ndf.createOrReplaceTempView("tab")
ndf=spark.sql("select empid,lastname from tab")
ndf.show()








'''df= spark.read.format("csv").option("header","true").option("inferSchama","true").load(data)
cols=[re.sub('[^a-zA-Z]',"",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)
ndf.createOrReplaceTempView("tab")
res=spark.sql("select empid,email from tab")
res.show(truncate=False)'''

