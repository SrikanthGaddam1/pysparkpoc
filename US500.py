from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
res=df.withColumn("todadate",current_date()).drop("phone1","phone2","email","web","address").withColumnRenamed("zip","sal")
res.createOrReplaceTempView("tab")
res1=spark.sql("select * from tab where state = 'LA' ")

#res1= res.filter(col("state")=="LA")

#res1.printSchema()
res1.show()

#spark submit