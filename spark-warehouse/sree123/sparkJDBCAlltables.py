from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext

host="jdbc:mysql://mysql.coeqadnju9kt.ap-south-1.rds.amazonaws.com:3306/venkatdb?useSSL=false"
df = spark.read.format("jdbc").option("url", host).option("dbtable", "DEPT").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").load()
df.show()