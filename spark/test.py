import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

def time():
    return datetime.datetime.now()

start = time()
#/home/shuaikun/spark-3.3.3-bin-hadoop3
# 创建SparkSession
spark = SparkSession.builder \
    .appName("spark_demo") \
    .config("spark.master", "spark://VM-0-32-ubuntu:7077") \
    .config("spark.driver.host", "192.168.3.238") \
    .config("spark.driver.bindAddress", "192.168.3.238") \
    .getOrCreate()

sc = spark.sparkContext

# 建立一个rdd
data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
rdd = sc.parallelize(data)

# 打印
print(rdd.collect())

# 停止SparkSession
spark.stop()

