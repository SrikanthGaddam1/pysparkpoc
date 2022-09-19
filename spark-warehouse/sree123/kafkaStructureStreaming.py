from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation","true")\
        .config("spark.driver.extraClassPath","C:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\*")\
        .config("spark.executor.extraClassPath","C:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\*")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "logs") \
        .option("startingOffsets", "earliest") \
        .load()
    data="C:\\Users\\SRIKANTH\\OneDrive\\Desktop\\litenav\\live"
    schema = spark.read.format("json").option("multiline", "true").load(data).schema
    schema1 = StructType([
        StructField("name", StringType()),
        StructField("age", LongType()),
        StructField("city", StringType())])
    #words_df = kafka_df.selectExpr("CAST(value AS STRING)")
    words_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("tmp"))
    res=words_df.withColumn("tmp1", explode(col("tmp.results"))).select(col("tmp1.user.cell"),col("tmp1.user.email"),col("tmp1.user.location.city"),col("tmp.nationality"), col("tmp.seed"),col("tmp.version"))

    #pkt_schema_string = "name string, age int, city STRING"
    #userSchema = StructType().add("name", "string").add("age", "integer").add("city","string")
    res.printSchema()


    def foreach_batch_function(df, epoch_id):
        # res1=df.withColumnRenamed("_c0","name").withColumnRenamed("_c1","age").withColumnRenamed("_c2","city")
        res = df
        # Send the dataframe into MongoDB which will create a BSON document out of it
        res.write \
            .format("jdbc") \
            .option("url",
                    "jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false") \
            .option("dbtable", "aug13logsrestapi") \
            .option("user", "myuser") \
            .option("password", "mypassword") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()

        pass


    '''res.writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .format("console") \
        .start() \
        .awaitTermination()'''

    res.writeStream \
        .trigger(processingTime='15 seconds')\
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()