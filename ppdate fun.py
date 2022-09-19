from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
sc = spark.sparkContext
data="D:\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

res=df.withColumn("dt",to_date(df.dt,"d-M-yyyy")).withColumn('today',current_date()).withColumn("ust",current_timestamp())
res.printSchema()
res.show(truncate=False)
