from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.streaming import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("ec2-52-66-110-175.ap-south-1.compute.amazonaws.com", 9999)
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#lines.pprint()
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        cols=["name","age","city"]
        df = rdd.map(lambda x:x.split(",")).toDF(cols)
        df.show()
        host="jdbc:mysql://mysqldb.coaugp4ng2j6.ap-south-1.rds.amazonaws.com:3306/mydbisaac"
        df.write.mode("append").format("jdbc")\
            .option("url",host)\
            .option("user","myuser")\
            .option("password","mypassword")\
            .option("driver","com.mysql.jdbc.Driver")\
            .option("dbtable","livedata").save()

    except:
        pass

lines.foreachRDD(process)


ssc.start()
ssc.awaitTermination()