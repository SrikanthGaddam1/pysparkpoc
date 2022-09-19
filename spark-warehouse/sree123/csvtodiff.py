from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\empdata.txt"
df=spark.read.format("csv").option("header","true").option("inferSchama","false").load(data)
import re
cols=[re.sub('[^a-zA-Z]','',x.lower()) for x in df.columns]
ndf=df.toDF(*cols)
res=ndf.select([regexp_replace(col(x),"\"","").alias(x) for x in ndf.columns])
res.show()
op="D:\\BIG DATA\\PYSPARK10-12\\"
#res.write.format("csv").option("header","true").save(op+"csvdata")
#res.write.format("orc").save(op+"orcdata")
#res.write.format("parquet").save(op+"parqdata")
#res.write.format("avro").save(op+"avrodt")