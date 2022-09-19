from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local[3]").config("spark.sql.streaming.forceDeleteTempCheckpointLocation","true").appName("test").getOrCreate()
sc = spark.sparkContext
lines = spark.readStream.format("socket").option("host", "ec2-52-66-110-175.ap-south-1.compute.amazonaws.com").option("port", 1235).load()
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()
query = wordCounts.writeStream.outputMode("complete").format("console").option("checkpointLocation","C:\\Users\SRIKANTH\\OneDrive\\Desktop\\litenav").start()

query.awaitTermination()