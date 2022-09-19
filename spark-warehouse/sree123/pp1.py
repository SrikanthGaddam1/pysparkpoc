from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:/bigdata/datasets/bank-full.csv"
df=spark.read.format("csv").option("header","true").option("sep", ";").option("inferSchema","true").load(data)
df.show(2)
df.printSchema()