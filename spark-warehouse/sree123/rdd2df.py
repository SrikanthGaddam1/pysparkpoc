from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\bigdata\\datasets\\email.txt"
ardd=sc.textFile(data)
pro = ardd.map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1])).filter(lambda x: "@" in x[1]).toDF(["name","email"])
#toDF() used convert rdd to dataframe .... also used to rename all columns.
res=pro.where(col("email").like("%yahoo%"))
res.show()
