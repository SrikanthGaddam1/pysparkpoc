from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
op="D:\\BIG DATA\\PYSPARK10-12\\"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(op+"csvdata")
ndf=df.na.fill(0,["comm"])
ndf.show()

host="jdbc:mysql://mysql.coeqadnju9kt.ap-south-1.rds.amazonaws.com:3306/venkatdb?useSSL=false"
ndf.write.format("jdbc").option("url", host).option("dbtable", "empcsvdata").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").save()
#avro data
df1=spark.read.format("avro").load(op+"avrodt")
ndf1=df1.na.fill(0,["comm"])
ndf1.show()
ndf1.write.format("jdbc").option("url", host).option("dbtable", "empavrodata").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").save()

#orc data
df2=spark.read.format("orc").load(op+"orcdata")
ndf2=df2.na.fill(0,["comm"])
ndf2.show()
ndf2.write.format("jdbc").option("url", host).option("dbtable", "emporcformat").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").save()

#parq data
df4=spark.read.format("parquet").load(op+"parqdata")
ndf4=df4.na.fill(0,["comm"])
ndf4.show()
ndf4.write.format("jdbc").option("url", host).option("dbtable", "empparq").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").save()


#res.write.format("orc").save(op+"orcdata")
#res.write.format("parquet").save(op+"parqdata")
#res.write.format("avro").save(op+"avrodt")