from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\empdata.txt"
df=spark.read.format("csv").option("inferSchama","true").option("header","true").load(data)
df.createOrReplaceTempView("emp")
qry="""select max(empid) as empid,deptid, max(sal) maxsal from emp group by deptid"""
res=spark.sql("0deptid, max(sal) maxsal from emp group by deptid, empid having maxsal=max(sal)")
res.show()

