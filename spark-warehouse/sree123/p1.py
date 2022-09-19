from pyspark.sql.functions import *
from pyspark.sql import *
import pandas
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#res=df[(df["state"]=="CA") & (col("email").contains("gmail"))]
res=df.groupBy(col("state")).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
#res=df[["state","email"]]
#df.show(30,truncate=False)
'''sql friendly'''
#df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where state ='NY' and email like'%gmail%'")
#res=spark.sql("select state, count(*) cnt from tab group by state order by cnt desc")
res.show(truncate=False)
