from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://mysql.coeqadnju9kt.ap-south-1.rds.amazonaws.com:3306/venkatdb?useSSL=false"
use="myuser"
pwd="mypassword"
#df=spark.read.format("jdbc").option("user",use).option("password",pwd).option("url",host)\
    #.option("driver","com.mysql.jdbc.Driver").option("dbtable","EMP").load()
'''df=spark.read.format("jdbc").option("url",host).option("dbtable","EMP").option("user",use).option("password",pwd).option("driver","com.mysql.jdbc.Driver").load()
df.printSchema()
#df.show()
#process Data
res=df.na.fill(0).withColumn("comm",col("comm").cast(IntegerType())).withColumn("hiredate",date_format(col("hiredate"),"yyyy-MMMM-dd"))
res.show()
res.write.mode("overwrite").format("jdbc").option("url",host).option("dbtable","EMPPOC").option("user",use).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").save()'''
df=spark.read.format("jdbc").option("url",host).option("dbtable","").option("user",use).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()
