from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\empdata.txt"
df=spark.read.format("csv").option("header","true").option("inferSchama","true").load(data)
import re
cols=[re.sub('[^a-zA-Z]','', x.lower()) for x in df.columns]
#toDF used to rename all columns
ndf=df.toDF(*cols)
#ndf.show()
res1=ndf.select([regexp_replace(col(x),"\"","").alias(x) for x in ndf.columns])
res1.show(truncate=False)
#spark unable to understand dd-MMM-yy format .. only understand yyyy-MM-dd format.
res=res1.withColumn("hiredate", to_date(ltrim(col("hiredate")),"d-MMM-yy"))\
    .withColumn("sal",col("sal").cast(IntegerType()))

res.printSchema()
#res.select(max(col("sal"))).show()
#res.withColumn((col("sal"))>=(max(col("sal")).alias("sal"))).show()
res.createOrReplaceTempView("tab")
#results = spark.sql("select * from tab where sal=(select max(sal) from tab)")
from pyspark.sql.window import *
win = Window.partitionBy(col("job")).orderBy(col("sal").asc())

results = res.withColumn("rnk",rank().over(win)).withColumn("drnk",dense_rank().over(win))\
.withColumn("rno",row_number().over(win))
results.show()
