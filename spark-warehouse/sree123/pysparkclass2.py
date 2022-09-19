from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
import re
data="C:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchama","false").load(data)
cols=[re.sub('[^a-zA-Z]','', x.lower()) for x in df.columns]
#toDF used to rename all columns
ndf=df.toDF(*cols)
def days2dt(dt):
    years = dt // 365
    months = (dt - years * 365) // 30
    days = (dt - years * 365 - months * 30)
    return days

udfdt = udf(days2dt)

res=ndf.withColumn("age",lit(18)).withColumn("phoneno",lit(123456)).withColumnRenamed("firstname","fname")\
    .withColumn("email", regexp_replace(col("email"),"yahoo","yyyyy"))\
    .withColumn("fullname", concat_ws(" ",col("nameprefix"),col("fname"), col("middleinitial"),col("lastname")))\
    .drop("nameprefix","fname","middleinitial","lastname")\
    .withColumn("dateofbirth", to_date(col("dateofbirth"),"M/d/yyyy")) \
    .withColumn("dateofjoining", to_date(col("dateofjoining"), "M/d/yyyy"))\
    .withColumn("dateyoung",datediff(col("dateofjoining"),col("dateofbirth")))\
    .withColumn("dateyoungdt",udfdt(col("dateyoung")))\
    .orderBy(col("dateyoung").asc())
#17 years 10 months 12 days ..


#datediff how many days diff between two dates ... in int (
#res1=res.select(sorted(res.columns,reverse=True))
res1=res.select(sorted(res.columns))
res1.printSchema()
#by default spark understand yyyy-MM-dd format only but u have M/d/yyyy thats y convert raw date to spark understandableformat use todate function

#res1=res.select([upper(col(x)).alias(x) for x in res.columns if "email" not in x])
#res1=res.select([regexp_replace(col(x),"\.","-").alias(x) for x in res.columns])
res1.show(truncate=False)

#drop .. used to delete column names from existing dataframe.
#withColumn used to add new column if that column not exists.
#withColumn used to update column values if that column already exists.
#lit .. used to add dummy data


