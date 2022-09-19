from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.master("local").config("spark.sql.session.timeZone", "UTC").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
'''res=df.withColumn("currentdate",current_date()).withColumn("ts",current_timestamp())\
    .withColumn("dt",to_date("dt","d-M-yyyy")).withColumn("dtdiss",datediff(col("currentdate"),col("dt")))\
    .withColumn("dateformate",date_format(col("dt"),"dd-LLL-yyyy-EEE"))\
    .withColumn("datweek",dayofweek(col("dt"))).withColumn("dayyer",dayofyear(col("dt"))).withColumn("datmonth",dayofmonth(col("dt")))\
    .withColumn("dayadd",date_add(col("dt"),-10)).withColumn("lastday",last_day(col("dt")))\
    .withColumn("nextday",next_day("currentdate","sat"))\
    .withColumn("lastfir",next_day(date_add(last_day("currentdate"),-7),"fri"))\
    .withColumn("lastsat",next_day(date_add(last_day("dt"),-7),"sat"))'''

df.createOrReplaceTempView("tab")
res=spark.sql("select * from tab")

res.show(truncate=False)
#res=df.where(col('amount')>=1000)
#next_day means from today onwards within next 7 working day whats the day ull get. means
#let eg: today 22-jul-22 is fri .. i want to know next friday use next_date(col("today"), "Friday") ... now it return 29-Jul-2022

#last_day of every month u ll get last day of that month ... means today 22-jul-2022 ..
# this month end so u ll get 31-Jul-2022.. if date id 20-feb-2022 .. it return feb 28

#date_add used to after specified num of days what date you will get .
# let eg: today 22-Jul-2022 if u add date_add(col("dt"),10) you will get 01-Aug-2022
#where as if u mention date_add(col("dt"),-10) minus value it return previous days means it return 12-Jul-2022

#dayofyear means .. how many days completed from jan-1-year ... today 22-07-2022 so from jan-1-2022 to today 210 days completed ..

#dayofweek ... 1-sun, 2-mon, 3-tue ...sat-7 days..means how many days from sunday
#dayofmonth ... how many days completed from month 1 date .means today date 22-07-2022 .. in jul 22 days completed. so u ll get 22

#date_format used to convert date as per ur expected format. date supported options like EEEE follow this link https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

#by default spark able to understand yyyy-MM-dd format so convert other format data to yyy-MM-dd to do that
#use to_date(col,input_dt_format)
#datediff(end,start) used to find number of days diff between two dates in Int format.
#current_date returns today date as per your system time/sparkSession time.
#to get proper results in sparkSession place this .config("spark.sql.session.timeZone", "EST")

