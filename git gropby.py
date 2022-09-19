from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext
data = "C:\\Users\\SRIKANTH\\Downloads\\myFile0.csv"
# spark_df = spark.createDataFrame(data)
df = spark.read.format("csv").option('header', 'true').load(data)
#def topSellingProductCountry(countryName):
    #data = spark_df.select(['productName', 'orderID']).where(spark_df.country == countryName).groupBy('productName').agg({'orderID': 'count'}).sort('count(orderID)').collect()
   # idList = [i[0] for i in data]
    #dataList = [i[1] for i in data]
df.show()