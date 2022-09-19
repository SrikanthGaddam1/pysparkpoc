# windows function when we use to like partishion by data [17 line]
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
res1.createOrReplaceTempView("tab")
#result=spark.sql("select * from tab where sal =(select max(SAL) from tab)").show()

from pyspark.sql.window import *

win=Window.partitionBy(col("job")).orderBy(col("sal").asc())
result=res1.withColumn("rnk",rank().over(win)).withColumn("dnk",dense_rank().over(win))\
    .withColumn("row no",row_number().over(win))
result.show()

