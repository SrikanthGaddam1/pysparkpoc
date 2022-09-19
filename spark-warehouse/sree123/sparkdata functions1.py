from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\empdata.txt"
df=spark.read.format("csv").option("header","true").option("inferSchama","true").load(data)
import re
cols=[re.sub('[^a-zA-Z]','',x.lower()) for x in df.columns]
ndf=df.toDF(*cols)
#ndf.show()
res=ndf.select([regexp_replace(col(x),'"','').alias(x) for x in ndf.columns])
res.show(truncate=False)
res1=res.withColumn('hiredate',to_date(ltrim(col("hiredate")),"d-MMM-yy"))
#res=res1.withColumn("hiredate", to_date(ltrim(col("hiredate")),"d-MMM-yy")).withColumn("sal",col("sal").cast(IntegerType()))
#res1.printSchema()
res1.show()

