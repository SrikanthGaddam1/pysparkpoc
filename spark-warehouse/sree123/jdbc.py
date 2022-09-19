from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local").getOrCreate()
url="jdbc:oracle:thin:@//chakridb.cpm0rg3ian3a.ap-south-1.rds.amazonaws.com:1521/ORCL"
qry = "delete table emp where pancard='' or pancard=none"
#df = spark.read.format("jdbc").option("url",url).option("user", "ouser").option("password","opassword").option("query","select * from EMP where sal>2000").option("driver","oracle.jdbc.OracleDriver").load()
df = spark.read.format("jdbc").option("url",url).option("user", "ouser")\
    .option("password","opassword").option("dbtable","EMP")\
    .option("fetchsize","3000").option("batchsize","5000").option("sessionInitStatement",qry)\
    .option("driver","oracle.jdbc.OracleDriver").load()

df.show()
df.printSchema()