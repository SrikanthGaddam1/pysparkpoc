from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
names=["srikanth","maths","phisics","chemistri"]
names.insert(0,"lol")
print(names)