# from pyspark.sql.functions import *
#from pyspark.sql import *

#spark = SparkSession.builder.master("local").appName("test").getOrCreate()
x=10
def f1():
    x=99
    def f2():
        x=88
       # print("value of x in f2:",x)
   # print("value of x in f1:",x)
def f3():
 print("value of x in f3:",x)

