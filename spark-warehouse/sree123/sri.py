from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local").appName("test").getOrCreate ()
data="C:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format('csv').option("header","True").load(data)
df.show()

