from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
ssc= StreamingContext(spark.sparkContext,10)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("ec2-15-207-108-224.ap-south-1.compute.amazonaws.com", 1234)
#lines.pprint()
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        df = rdd.map(lambda x:x.split(",")).toDF(['name','age','city'])
        df.show()
        abcd = "jdbc:oracle:thin:@//chakridb.cpm0rg3ian3a.ap-south-1.rds.amazonaws.com:1521/ORCL"
        df.write.mode("append").format("jdbc").option("url", "abcd").option("user", "ouser").option("password", "opassword").option("dbtable", "chakrilivedata").option("driver", "oracle.jdbc.OracleDriver").save()

    except:
        pass

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()