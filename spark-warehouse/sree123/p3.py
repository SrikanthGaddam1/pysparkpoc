import ast

from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data = "C:\\bigdata\\datasets\\Student.xml"
import re
df = spark.read.format("xml").option("rowTag","Source_Data").load(data)
cols=[re.sub('[^a-zA-Z1-9]','',a.upper()) for a in df.columns]
df1=df.toDF(*cols)
df2=df1.select([regexp_replace(col(x),'[$,]','').alias(x) for x in df1.columns]).na.fill("0").withColumn("all",col("QTR1")+col("QTR2")+col("QTR3")+col("QTR4"))

#df1=df.select([regexp_replace(col(x),'[$,]','').alias(x) for x in df.columns]).na.fill("0")
#cols = [re.sub('[^a-zA-Z]','',x.upper()) for x in df.columns]
#df1 = df.toDF(*cols)
#ndf = df1.na.fill("0",["COMM"])
df2.show()
