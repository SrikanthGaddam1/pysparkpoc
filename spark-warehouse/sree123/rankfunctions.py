from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\archive\\StudentsPerformance.csv"

df=spark.read.format("csv").option("header","true").option("inferSchama","true").load(data)
import re
cols=[re.sub('[^a-zA-Z]','', x.lower()) for x in df.columns]
#toDF used to rename all columns
ndf=df.toDF(*cols).withColumn("total",col("mathscore")+col("readingscore")+col("writingscore")).orderBy(col("total").desc())
from pyspark.sql.window import *
win = Window.partitionBy(col("raceethnicity")).orderBy(col("total").desc())

res=ndf.withColumn("rank",rank().over(win)).withColumn("drank",dense_rank().over(win))\
.withColumn("rownum",row_number().over(win)).where(col("drank")<=10).drop("parentallevelofeducation","testpreparationcourse")\
.withColumn("ntile",ntile(5).over(win)).withColumn("per",percent_rank().over(win))\
.withColumn("lead",lead(col("total")).over(win)).withColumn("lag",lag(col("total")).over(win)).withColumn("diff",col("total")-col("lead"))\

res.show(200)
