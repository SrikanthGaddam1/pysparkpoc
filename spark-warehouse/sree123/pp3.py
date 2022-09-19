from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").load(data)

#df.createOrReplaceTempView("tab")
#res=df.where((col("state")=="NJ") & (col("email").contains("yahoo")))
#res=df.groupBy(col("state")).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
res=df.groupBy(col("zip")).agg(max("zip")).orderBy(col("max(zip)").desc())
#res=spark.sql("select * from tab where state='CA' and email like '%yahoo%'")
#res=spark.sql("select state, count(*) cnt from tab group by state order by state asc")
res.show(truncate=False)
