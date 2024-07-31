from operator import add
from pyspark.sql import SparkSession
import random
# 创建SparkSession
spark = SparkSession.builder \
    .appName("wsk_spark_test_cpu: Pi Calculation") \
    .config("spark.master", "spark://VM-0-32-ubuntu:7077") \
    .config("spark.driver.host", "192.168.3.238") \
    .config("spark.driver.bindAddress", "192.168.3.238") \
    .getOrCreate()

sc = spark.sparkContext

# 设置分片数


partitions = 10
n = 100000 * partitions

def f(_: int) -> float:
    x = random.random() * 2 - 1
    y = random.random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
pi = 4.0 * count / n

print(f"Pi is roughly {pi}")
# 停止SparkSession
spark.stop()
