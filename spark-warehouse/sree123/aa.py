from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\bank-full.csv"
#create rdd
rdd=sc.textFile(data)
fst=rdd.first()
#res=rdd.filter(lambda x:x!=fst).map(lambda x:x.replace("\"","")).map(lambda x:x.split(";")).filter(lambda x:int(x[5])>50000)
#res=rdd.filter(lambda x:x!=fst).map(lambda x:x.replace("\"","")).map(lambda x:x.split(";")).filter(lambda x:(x[2]!='married') & (int(x[5])>50000))
#select * from tab where sal>50000 and marital='single'
#res=rdd.filter(lambda x:x!=fst).map(lambda x:x.replace("\"","")).map(lambda x:x.split(";")).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
res=rdd.filter(lambda x:x!=fst).map(lambda x:x.replace("\"","")).map(lambda x:x.split(";")).map(lambda x:(x[2],int(x[5]))).reduceByKey(lambda x,y:x+y)
#select marital, count(*) from tab group by marital;


for i in res.take(29):
    print(i)

#df=spark.read.format("csv").option("header","true").load(data)
#df.show()