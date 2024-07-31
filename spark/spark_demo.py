from time import sleep
from pyspark.sql import SparkSession

import datetime

def time():
    return datetime.datetime.now()

start = time()

# 创建SparkSession
spark = SparkSession.builder \
    .appName("1") \
    .getOrCreate()
sc = spark.sparkContext
# 建立一个rdd
data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
rdd = sc.parallelize(data)
# sleep(10)
# 打印
print(rdd.collect())