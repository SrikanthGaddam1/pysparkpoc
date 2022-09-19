from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

data="C:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option("inferSchema","true").option("header","true").option("sep",",").load(data)
def grade(x):
    if(x>=1000 and x<5000):
        return "low donations"
    elif(x>=5000 and x<8000):
        return "good donations"
    else:
        return "high donations"

#python function convert to udf
gradeudf = udf(grade)

ndf=df.withColumn("donationGrade", gradeudf(col("amount")))
ndf.show()






