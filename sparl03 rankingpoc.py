from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\archive\\StudentsPerformance.csv"
df=spark.read.format("csv").option("header","true").option("header","true").load(data)
import re
cols=[re.sub('[^a-zA-Z]','', x.lower()) for x in df.columns]
#toDF used to rename all columns
ndf=df.toDF(*cols)
ndf1=ndf.withColumn("total",col("mathscore")+col("readingscore")+col("writingscore")).orderBy(col("total").desc())
from pyspark.sql.window import *
win=Window.partitionBy(col("raceethnicity")).orderBy(col("total").desc())
ndf2=ndf1.withColumn("rnk",rank().over(win)).withColumn("drnk",dense_rank().over(win)).withColumn("rno",row_number().over(win))\
    .where(col("drnk")<=10).drop("lunch","testpreparationcourse").withColumn("ntile",ntile(5).over(win)).withColumn("persentage",percent_rank().over(win))\
    .withColumn("lead",lead(col("total")).over(win)).withColumn("lar",lag(col("total")).over(win)).withColumn("persentage",round("persentage",3))\
    .withColumn("total",col("total").cast(IntegerType()))

ndf2.show(200)