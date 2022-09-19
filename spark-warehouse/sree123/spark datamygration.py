from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://mysql.coeqadnju9kt.ap-south-1.rds.amazonaws.com:3306/venkatdb?useSSL=false"
df = spark.read.format("jdbc").option("url", host).option("dbtable", "srikanthemp").option("user","myuser")\
.option("driver","com.mysql.jdbc.Driver").option("password","mypassword").load()
res=df.na.fill(0,["comm"]).withColumn("hiredate",date_format(col("hiredate"),"yyyy-MMMM-dd"))
#res=df.na.fill(0)
res1=df.withColumn('hiredate',to_date(ltrim(col("hiredate")),"yyyy-MMMM-dd"))
res1.show()

#res.write.format("jdbc").option("url", host).option("dbtable", "srikanthemp").option("user","myuser")\
#.option("driver","com.mysql.jdbc.Driver").option("password","mypassword").save()