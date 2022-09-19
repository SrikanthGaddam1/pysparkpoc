from pyspark.sql.functions import *
from pyspark.sql import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").load(data)
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
df1=df.toDF(*cols)
#res=df1.groupBy(df1.state).agg(count("*").alias("cnt"),collect_list(df1.firstname).alias("name")).orderBy(col("cnt").desc())
res=df1.groupBy(df1.state).agg(count("*").alias("cnt"),collect_set(df1.city).alias("name")).orderBy(col("cnt").desc())
#res=df1.withColumn("fullname",concat_ws(" ",col("firstname"),col("lastname")))
res.show(truncate=False)
res.printSchema()